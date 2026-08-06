//! JoinNode for deterministic fan-in merging.
//!
//! When parallel branches converge, a [`JoinNode`] provides explicit
//! merge logic. It reads specified keys from state (set by parallel branches),
//! applies a merge function, and writes the result to an output key.

use crate::command::NodeOutput;
use crate::config::GraphConfig;
use crate::error::AgentGraphError;
use crate::node::Node;
use crate::state::AgentState;
use serde_json::Value;

/// Merge function signature for JoinNode.
pub type MergeFn = Box<dyn Fn(Vec<(String, Value)>) -> crate::Result<Value> + Send + Sync>;

/// A node that merges results from parallel branches.
///
/// After fan-out, parallel branches write their results to known state keys.
/// The JoinNode reads those keys, applies a merge function, and writes
/// the merged result to an output key.
pub struct JoinNode {
    name: Option<String>,
    /// State keys to read from parallel branches.
    input_keys: Vec<String>,
    /// State key to write the merged result to.
    output_key: String,
    /// Merge function: receives `Vec<(key, value)>` and produces the merged value.
    merge_fn: MergeFn,
}

impl JoinNode {
    /// Create a new JoinNode.
    ///
    /// - `input_keys`: state keys to collect from parallel branches.
    /// - `output_key`: state key to write the merged result to.
    /// - `merge_fn`: function that merges the collected values.
    pub fn new(
        input_keys: Vec<String>,
        output_key: impl Into<String>,
        merge_fn: impl Fn(Vec<(String, Value)>) -> crate::Result<Value> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: None,
            input_keys,
            output_key: output_key.into(),
            merge_fn: Box::new(merge_fn),
        }
    }

    /// Set a name for this node.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Convenience: create a JoinNode that collects values into an array.
    pub fn collect_array(input_keys: Vec<String>, output_key: impl Into<String>) -> Self {
        Self::new(input_keys, output_key, |values| {
            let arr: Vec<Value> = values.into_iter().map(|(_, v)| v).collect();
            Ok(Value::Array(arr))
        })
    }

    /// Convenience: create a JoinNode that merges objects (shallow).
    pub fn merge_objects(input_keys: Vec<String>, output_key: impl Into<String>) -> Self {
        Self::new(input_keys, output_key, |values| {
            let mut result = serde_json::Map::new();
            for (key, value) in values {
                if let Value::Object(map) = value {
                    for (k, v) in map {
                        result.insert(k, v);
                    }
                } else {
                    result.insert(key, value);
                }
            }
            Ok(Value::Object(result))
        })
    }

    /// Convenience: create a JoinNode that collects values into an object
    /// keyed by branch state key (preserves branch identity).
    ///
    /// Branch IDs must be stable and cannot collide: passing the same state
    /// key twice is a hard error.
    pub fn collect_object(input_keys: Vec<String>, output_key: impl Into<String>) -> Self {
        Self::new(input_keys, output_key, |values| {
            let mut result = serde_json::Map::new();
            let mut seen = std::collections::BTreeSet::new();
            for (key, value) in values {
                if !seen.insert(key.clone()) {
                    return Err(AgentGraphError::ExecutionError(format!(
                        "collect_object: duplicate branch key '{key}'"
                    )));
                }
                result.insert(key, value);
            }
            Ok(Value::Object(result))
        })
    }

    /// Create a JoinNode that executes a five-stage [`JoinStrategy`] pipeline:
    /// validate inputs → normalize → expose contradictions → adjudicate →
    /// certify. The terminal value is an envelope object so that certification
    /// status, contradictions, and minority reports survive the join:
    ///
    /// ```json
    /// {
    ///   "join": "<strategy>",
    ///   "certification": "pass|abstain|quarantine|request_authority",
    ///   "value": ...,
    ///   "contradictions": [...],
    ///   "minority_report": [...],
    ///   "notes": [...]
    /// }
    /// ```
    ///
    /// A `Fail` certification aborts graph execution; `Quarantine`, `Abstain`,
    /// and `RequestAuthority` complete with the envelope so downstream nodes
    /// can observe the terminal status.
    pub fn strategy(
        input_keys: Vec<String>,
        output_key: impl Into<String>,
        strategy: Box<dyn JoinStrategy>,
    ) -> Self {
        let join_name = strategy.name().to_owned();
        Self::new(input_keys, output_key, move |values| {
            strategy
                .validate_inputs(&values)
                .map_err(AgentGraphError::ExecutionError)?;
            let set = strategy
                .normalize(values)
                .map_err(AgentGraphError::ExecutionError)?;
            let contradictions = strategy
                .contradictions(&set)
                .map_err(AgentGraphError::ExecutionError)?;
            let outcome = strategy
                .adjudicate(set, &contradictions)
                .map_err(AgentGraphError::ExecutionError)?;
            let outcome = strategy
                .certify(outcome)
                .map_err(AgentGraphError::ExecutionError)?;
            match outcome.certification {
                JoinCertification::Fail => Err(AgentGraphError::ExecutionError(format!(
                    "join '{join_name}' failed certification"
                ))),
                _ => Ok(serde_json::json!({
                    "join": join_name,
                    "certification": outcome.certification,
                    "value": outcome.value,
                    "contradictions": outcome.contradictions,
                    "minority_report": outcome.minority_report,
                    "notes": outcome.notes,
                })),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Join strategy plugin trait (five-stage pipeline)
// ---------------------------------------------------------------------------

/// Terminal certification for a strategy join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinCertification {
    Pass,
    Fail,
    Abstain,
    Quarantine,
    RequestAuthority,
}

/// Outcome of a strategy join: certified terminal payload plus preserved
/// contradictions, minority reports, and notes.
#[derive(Debug, Clone, serde::Serialize)]
pub struct JoinOutcome {
    pub certification: JoinCertification,
    pub value: Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contradictions: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub minority_report: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

/// Five-stage join strategy: validate → normalize → contradictions →
/// adjudicate → certify. Deterministic and dependency-free; all input is
/// `serde_json::Value` branch artifacts.
pub trait JoinStrategy: Send + Sync {
    fn name(&self) -> &'static str;

    fn validate_inputs(&self, inputs: &[(String, Value)]) -> Result<(), String>;

    fn normalize(&self, inputs: Vec<(String, Value)>) -> Result<Vec<(String, Value)>, String>;

    fn contradictions(&self, set: &[(String, Value)]) -> Result<Vec<Value>, String>;

    fn adjudicate(
        &self,
        set: Vec<(String, Value)>,
        contradictions: &[Value],
    ) -> Result<JoinOutcome, String>;

    fn certify(&self, outcome: JoinOutcome) -> Result<JoinOutcome, String> {
        Ok(outcome)
    }
}

/// Collapse duplicate findings by stable artifact/claim identity rather than
/// wording. Identity is the `identity_path` string field when configured,
/// otherwise the branch state key itself. The first occurrence wins.
pub struct DedupeByIdentity {
    pub identity_path: Option<String>,
}

/// Emit explicit claim pairs that cannot both hold under the same scope and
/// time window. Claims missing scope or time are not judged.
pub struct ContradictionMatrix {
    pub scope_path: String,
    pub claim_path: String,
    pub time_path: String,
    /// Quarantine (rather than pass) when contradictions are found.
    pub strict: bool,
}

impl Default for ContradictionMatrix {
    fn default() -> Self {
        Self {
            scope_path: "scope".into(),
            claim_path: "claim".into(),
            time_path: "time".into(),
            strict: false,
        }
    }
}

/// Retain high-value dissent after convergence. Artifacts flagged
/// `dissent: true` are preserved in `minority_report` and excluded from the
/// majority value.
pub struct MinorityReport {
    pub dissent_path: String,
}

impl Default for MinorityReport {
    fn default() -> Self {
        Self {
            dissent_path: "dissent".into(),
        }
    }
}

/// Advance only outputs carrying required schema fields, source witnesses,
/// executed checks, and receipts. Invalid artifacts quarantine the join
/// (visible terminal state) instead of failing the graph.
pub struct ProofCarryingJoin {
    pub required_fields: Vec<String>,
}

impl Default for ProofCarryingJoin {
    fn default() -> Self {
        Self {
            required_fields: vec!["evidence".into(), "checks".into(), "receipt".into()],
        }
    }
}

fn path_get<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

fn claim_of(value: &Value, claim_path: &str) -> Option<String> {
    match path_get(value, claim_path) {
        Some(Value::String(s)) => Some(s.clone()),
        Some(other) => Some(other.to_string()),
        None => None,
    }
}

impl JoinStrategy for DedupeByIdentity {
    fn name(&self) -> &'static str {
        "dedupe_by_identity"
    }

    fn validate_inputs(&self, inputs: &[(String, Value)]) -> Result<(), String> {
        if inputs.is_empty() {
            return Err("dedupe_by_identity requires at least one branch artifact".into());
        }
        if let Some(path) = &self.identity_path {
            for (key, value) in inputs {
                if path_get(value, path).is_none() {
                    return Err(format!(
                        "dedupe_by_identity: artifact '{key}' is missing identity path '{path}'"
                    ));
                }
            }
        }
        Ok(())
    }

    fn normalize(&self, inputs: Vec<(String, Value)>) -> Result<Vec<(String, Value)>, String> {
        let mut seen = std::collections::BTreeSet::new();
        let mut out = Vec::new();
        for (key, value) in inputs {
            let identity = self
                .identity_path
                .as_deref()
                .and_then(|path| path_get(&value, path))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| key.clone());
            if seen.insert(identity) {
                out.push((key, value));
            }
        }
        Ok(out)
    }

    fn contradictions(&self, _set: &[(String, Value)]) -> Result<Vec<Value>, String> {
        Ok(Vec::new())
    }

    fn adjudicate(
        &self,
        set: Vec<(String, Value)>,
        _contradictions: &[Value],
    ) -> Result<JoinOutcome, String> {
        let values = set.into_iter().map(|(_, value)| value).collect();
        Ok(JoinOutcome {
            certification: JoinCertification::Pass,
            value: Value::Array(values),
            contradictions: Vec::new(),
            minority_report: Vec::new(),
            notes: Vec::new(),
        })
    }
}

impl JoinStrategy for ContradictionMatrix {
    fn name(&self) -> &'static str {
        "contradiction_matrix"
    }

    fn validate_inputs(&self, inputs: &[(String, Value)]) -> Result<(), String> {
        if inputs.is_empty() {
            return Err("contradiction_matrix requires at least one branch artifact".into());
        }
        Ok(())
    }

    fn normalize(&self, inputs: Vec<(String, Value)>) -> Result<Vec<(String, Value)>, String> {
        Ok(inputs)
    }

    fn contradictions(&self, set: &[(String, Value)]) -> Result<Vec<Value>, String> {
        let mut out = Vec::new();
        for left in 0..set.len() {
            for right in (left + 1)..set.len() {
                let (l_key, l_value) = &set[left];
                let (r_key, r_value) = &set[right];
                let (Some(l_scope), Some(r_scope)) = (
                    path_get(l_value, &self.scope_path),
                    path_get(r_value, &self.scope_path),
                ) else {
                    continue;
                };
                let (Some(l_time), Some(r_time)) = (
                    path_get(l_value, &self.time_path),
                    path_get(r_value, &self.time_path),
                ) else {
                    continue;
                };
                if l_scope != r_scope || l_time != r_time {
                    continue;
                }
                let (Some(l_claim), Some(r_claim)) = (
                    claim_of(l_value, &self.claim_path),
                    claim_of(r_value, &self.claim_path),
                ) else {
                    continue;
                };
                if l_claim == r_claim {
                    continue;
                }
                out.push(serde_json::json!({
                    "left": l_key,
                    "right": r_key,
                    "scope": l_scope,
                    "time": l_time,
                    "left_claim": l_claim,
                    "right_claim": r_claim,
                }));
            }
        }
        Ok(out)
    }

    fn adjudicate(
        &self,
        set: Vec<(String, Value)>,
        contradictions: &[Value],
    ) -> Result<JoinOutcome, String> {
        let values = set.into_iter().map(|(_, value)| value).collect();
        let mut notes = Vec::new();
        if !contradictions.is_empty() {
            notes.push(format!(
                "{} contradiction pair(s) exposed",
                contradictions.len()
            ));
        }
        Ok(JoinOutcome {
            certification: JoinCertification::Pass,
            value: Value::Array(values),
            contradictions: contradictions.to_vec(),
            minority_report: Vec::new(),
            notes,
        })
    }

    fn certify(&self, outcome: JoinOutcome) -> Result<JoinOutcome, String> {
        if self.strict && !outcome.contradictions.is_empty() {
            let mut notes = outcome.notes;
            notes.push("strict mode: quarantined on contradiction".into());
            Ok(JoinOutcome {
                certification: JoinCertification::Quarantine,
                notes,
                ..outcome
            })
        } else {
            Ok(outcome)
        }
    }
}

impl JoinStrategy for MinorityReport {
    fn name(&self) -> &'static str {
        "minority_report"
    }

    fn validate_inputs(&self, inputs: &[(String, Value)]) -> Result<(), String> {
        if inputs.is_empty() {
            return Err("minority_report requires at least one branch artifact".into());
        }
        Ok(())
    }

    fn normalize(&self, inputs: Vec<(String, Value)>) -> Result<Vec<(String, Value)>, String> {
        Ok(inputs)
    }

    fn contradictions(&self, _set: &[(String, Value)]) -> Result<Vec<Value>, String> {
        Ok(Vec::new())
    }

    fn adjudicate(
        &self,
        set: Vec<(String, Value)>,
        _contradictions: &[Value],
    ) -> Result<JoinOutcome, String> {
        let mut majority = Vec::new();
        let mut minority = Vec::new();
        for (_, value) in set {
            let dissent = path_get(&value, &self.dissent_path)
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if dissent {
                minority.push(value);
            } else {
                majority.push(value);
            }
        }
        let mut notes = Vec::new();
        if !minority.is_empty() {
            notes.push(format!(
                "{} dissenting artifact(s) preserved in minority report",
                minority.len()
            ));
        }
        Ok(JoinOutcome {
            certification: JoinCertification::Pass,
            value: Value::Array(majority),
            contradictions: Vec::new(),
            minority_report: minority,
            notes,
        })
    }
}

fn evidence_valid(value: &Value) -> Result<(), String> {
    let Some(entries) = value.get("evidence").and_then(Value::as_array) else {
        return Ok(()); // field presence is enforced via required_fields
    };
    for entry in entries {
        let witness_id = entry.get("witness_id").and_then(Value::as_str);
        let digest = entry.get("digest").and_then(Value::as_str);
        match (witness_id, digest) {
            (Some(w), Some(d)) if !w.is_empty() && !d.is_empty() => {}
            _ => return Err("evidence entry must carry non-empty witness_id and digest".into()),
        }
    }
    Ok(())
}

fn checks_valid(value: &Value) -> Result<(), String> {
    let Some(entries) = value.get("checks").and_then(Value::as_array) else {
        return Ok(()); // field presence is enforced via required_fields
    };
    if entries.is_empty() {
        return Err("checks must not be empty".into());
    }
    for entry in entries {
        let status = entry.get("status").and_then(Value::as_str);
        if status != Some("passed") {
            return Err(
                "every check must carry status \"passed\" (executed check receipts only)".into(),
            );
        }
    }
    Ok(())
}

impl JoinStrategy for ProofCarryingJoin {
    fn name(&self) -> &'static str {
        "proof_carrying_join"
    }

    fn validate_inputs(&self, inputs: &[(String, Value)]) -> Result<(), String> {
        if inputs.is_empty() {
            return Err("proof_carrying_join requires at least one branch artifact".into());
        }
        Ok(())
    }

    fn normalize(&self, inputs: Vec<(String, Value)>) -> Result<Vec<(String, Value)>, String> {
        Ok(inputs)
    }

    fn contradictions(&self, _set: &[(String, Value)]) -> Result<Vec<Value>, String> {
        Ok(Vec::new())
    }

    fn adjudicate(
        &self,
        set: Vec<(String, Value)>,
        _contradictions: &[Value],
    ) -> Result<JoinOutcome, String> {
        let mut valid = Vec::new();
        let mut notes = Vec::new();
        let mut invalid = 0usize;
        for (key, value) in set {
            let mut reasons = Vec::new();
            for field in &self.required_fields {
                if path_get(&value, field).is_none() {
                    reasons.push(format!("missing required field '{field}'"));
                }
            }
            if let Err(e) = evidence_valid(&value) {
                reasons.push(e);
            }
            if let Err(e) = checks_valid(&value) {
                reasons.push(e);
            }
            if reasons.is_empty() {
                valid.push(value);
            } else {
                invalid += 1;
                notes.push(format!(
                    "artifact '{key}' quarantined: {}",
                    reasons.join("; ")
                ));
            }
        }
        let certification = if invalid == 0 {
            JoinCertification::Pass
        } else {
            JoinCertification::Quarantine
        };
        Ok(JoinOutcome {
            certification,
            value: Value::Array(valid),
            contradictions: Vec::new(),
            minority_report: Vec::new(),
            notes,
        })
    }
}

#[async_trait::async_trait]
impl Node for JoinNode {
    async fn execute(
        &self,
        state: &AgentState,
        _config: &GraphConfig,
    ) -> crate::Result<NodeOutput> {
        // Collect values from input keys
        let mut inputs = Vec::new();
        for key in &self.input_keys {
            let value: Value = state.get_opt::<Value>(key).await?.unwrap_or(Value::Null);
            inputs.push((key.clone(), value));
        }

        // Validate that we have at least some non-null inputs
        let has_data = inputs.iter().any(|(_, v)| !v.is_null());
        if !has_data {
            return Err(AgentGraphError::ExecutionError(format!(
                "JoinNode: no data found for input keys {:?}",
                self.input_keys
            )));
        }

        // Apply merge function
        let merged = (self.merge_fn)(inputs)?;

        // Write to output key
        state.set_raw(&self.output_key, merged).await?;

        Ok(NodeOutput::Done)
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

impl std::fmt::Debug for JoinNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinNode")
            .field("name", &self.name)
            .field("input_keys", &self.input_keys)
            .field("output_key", &self.output_key)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn artifact(key: &str, value: Value) -> (String, Value) {
        (key.to_owned(), value)
    }

    #[test]
    fn dedupe_by_identity_collapses_duplicate_ids() {
        let strategy = DedupeByIdentity {
            identity_path: Some("claim.id".into()),
        };
        let inputs = vec![
            artifact("a", json!({"claim": {"id": "C1"}, "finding": "x"})),
            artifact("b", json!({"claim": {"id": "C1"}, "finding": "x"})),
            artifact("c", json!({"claim": {"id": "C2"}, "finding": "y"})),
        ];
        strategy.validate_inputs(&inputs).unwrap();
        let set = strategy.normalize(inputs).unwrap();
        assert_eq!(
            set.len(),
            2,
            "duplicate identity must collapse to first occurrence"
        );
        assert_eq!(set[0].0, "a");
        assert_eq!(set[1].0, "c");
    }

    #[test]
    fn dedupe_rejects_missing_identity_path() {
        let strategy = DedupeByIdentity {
            identity_path: Some("claim.id".into()),
        };
        let inputs = vec![artifact("a", json!({"finding": "x"}))];
        let err = strategy.validate_inputs(&inputs).unwrap_err();
        assert!(err.contains("missing identity path"));
    }

    #[test]
    fn contradiction_matrix_exposes_same_scope_time_conflict() {
        let strategy = ContradictionMatrix::default();
        let inputs = vec![
            artifact(
                "a",
                json!({"scope": "s1", "time": "2026-08-06", "claim": "true"}),
            ),
            artifact(
                "b",
                json!({"scope": "s1", "time": "2026-08-06", "claim": "false"}),
            ),
        ];
        let contradictions = strategy.contradictions(&inputs).unwrap();
        assert_eq!(contradictions.len(), 1);
        assert_eq!(contradictions[0]["left"], "a");
        assert_eq!(contradictions[0]["right"], "b");
        assert_eq!(contradictions[0]["left_claim"], "true");
        assert_eq!(contradictions[0]["right_claim"], "false");
    }

    #[test]
    fn contradiction_matrix_temporal_mismatch_is_not_contradictory() {
        let strategy = ContradictionMatrix::default();
        let inputs = vec![
            artifact(
                "a",
                json!({"scope": "s1", "time": "2026-08-06", "claim": "true"}),
            ),
            artifact(
                "b",
                json!({"scope": "s1", "time": "2026-08-07", "claim": "false"}),
            ),
        ];
        let contradictions = strategy.contradictions(&inputs).unwrap();
        assert!(
            contradictions.is_empty(),
            "claims valid at different times are not contradictory"
        );
    }

    #[test]
    fn contradiction_matrix_strict_quarantines() {
        let strategy = ContradictionMatrix {
            strict: true,
            ..Default::default()
        };
        let inputs = vec![
            artifact("a", json!({"scope": "s1", "time": "t", "claim": "true"})),
            artifact("b", json!({"scope": "s1", "time": "t", "claim": "false"})),
        ];
        let contradictions = strategy.contradictions(&inputs).unwrap();
        let outcome = strategy.adjudicate(inputs, &contradictions).unwrap();
        let outcome = strategy.certify(outcome).unwrap();
        assert_eq!(outcome.certification, JoinCertification::Quarantine);
    }

    #[test]
    fn minority_report_preserves_dissent() {
        let strategy = MinorityReport::default();
        let inputs = vec![
            artifact("a", json!({"dissent": false, "claim": "x"})),
            artifact("b", json!({"dissent": true, "claim": "y"})),
            artifact("c", json!({"claim": "x"})),
        ];
        let outcome = strategy.adjudicate(inputs, &[]).unwrap();
        assert_eq!(outcome.value.as_array().unwrap().len(), 2);
        assert_eq!(outcome.minority_report.len(), 1);
        assert_eq!(outcome.minority_report[0]["claim"], "y");
    }

    #[test]
    fn proof_carrying_join_passes_valid_artifacts() {
        let strategy = ProofCarryingJoin::default();
        let inputs = vec![artifact(
            "a",
            json!({
                "evidence": [{"witness_id": "w1", "digest": "sha256:abc"}],
                "checks": [{"status": "passed"}],
                "receipt": "receipt:r1",
            }),
        )];
        let outcome = strategy.adjudicate(inputs, &[]).unwrap();
        assert_eq!(outcome.certification, JoinCertification::Pass);
        assert_eq!(outcome.value.as_array().unwrap().len(), 1);
    }

    #[test]
    fn proof_carrying_join_quarantines_invalid_evidence_reference() {
        let strategy = ProofCarryingJoin::default();
        let inputs = vec![artifact(
            "a",
            json!({
                "evidence": [{"locator": "https://example.com/x"}],
                "checks": [{"status": "passed"}],
                "receipt": "receipt:r1",
            }),
        )];
        let outcome = strategy.adjudicate(inputs, &[]).unwrap();
        assert_eq!(outcome.certification, JoinCertification::Quarantine);
        assert!(outcome.value.as_array().unwrap().is_empty());
        assert!(outcome.notes.iter().any(|n| n.contains("quarantined")));
    }

    #[test]
    fn proof_carrying_join_quarantines_unexecuted_check() {
        let strategy = ProofCarryingJoin::default();
        let inputs = vec![artifact(
            "a",
            json!({
                "evidence": [{"witness_id": "w1", "digest": "sha256:abc"}],
                "checks": [{"status": "running"}],
                "receipt": "receipt:r1",
            }),
        )];
        let outcome = strategy.adjudicate(inputs, &[]).unwrap();
        assert_eq!(outcome.certification, JoinCertification::Quarantine);
    }

    #[test]
    fn proof_carrying_join_quarantines_missing_receipt() {
        let strategy = ProofCarryingJoin::default();
        let inputs = vec![artifact(
            "a",
            json!({
                "evidence": [{"witness_id": "w1", "digest": "sha256:abc"}],
                "checks": [{"status": "passed"}],
            }),
        )];
        let outcome = strategy.adjudicate(inputs, &[]).unwrap();
        assert_eq!(outcome.certification, JoinCertification::Quarantine);
        assert!(outcome
            .notes
            .iter()
            .any(|n| n.contains("missing required field 'receipt'")));
    }
}
