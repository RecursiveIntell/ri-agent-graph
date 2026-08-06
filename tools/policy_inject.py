#!/usr/bin/env python3
"""Policy injection — makes every materialized blueprint carry its catalog governance.

For each blueprint in MATERIALIZED (except B06, already policy-complete), load the
registered-style spec from --src and inject the catalog's governance fields into prompts:
  - json_mode=True (pre-join analyst/branch) nodes → CONTEXT POLICY + TOOL POLICY + ANTI-PATTERNS
  - json_mode=False (post-join synth/judge) nodes → STOP CONDITIONS + RECEIPT REQUIREMENTS + ANTI-PATTERNS
Outputs policy-complete specs to --out (default /tmp/swarm-blueprints/policy).

Usage: policy_inject.py [--catalog <catalog.json>] [--src <dir>] [--out <dir>]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

MATERIALIZED = ["B00", "B01", "B02", "B04", "B07", "B10", "B11", "B13", "B14", "B15", "B16", "B19", "B20"]
PREFIX = "swarm-B"
SUFFIX = "-20260806"


def policies_for(bp: dict) -> dict:
    def t(field: str) -> str:
        v = bp.get(field)
        if v is None:
            return ""
        if isinstance(v, list):
            return "; ".join(str(x) for x in v)
        return str(v)
    return {k: t(k) for k in ("context_policy", "tool_policy", "stop_conditions", "receipt_requirements", "anti_patterns")}


def inject(spec: dict, bp: dict) -> dict:
    pol = policies_for(bp)
    anti = pol["anti_patterns"]
    for node in spec.get("nodes", []):
        if node.get("type") != "llm" or not node.get("prompt"):
            continue
        extra = []
        if node.get("json_mode"):
            for f in ("context_policy", "tool_policy"):
                if pol[f]:
                    extra.append(f"{f.replace('_', ' ').upper()}: {pol[f]}")
        else:
            for f in ("stop_conditions", "receipt_requirements"):
                if pol[f]:
                    extra.append(f"{f.replace('_', ' ').upper()}: {pol[f]}")
        if anti:
            extra.append(f"ANTI-PATTERNS: {anti}")
        if extra:
            node["prompt"] = node["prompt"].rstrip() + " " + " ".join(extra)
    return spec


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", type=Path, default=Path("/home/sikmindz/Downloads/swarm-bundle/RecursiveIntell_Swarm_Blueprint_Catalog_v1.json"))
    ap.add_argument("--src", type=Path, default=Path("/tmp/swarm-blueprints"))
    ap.add_argument("--out", type=Path, default=Path("/tmp/swarm-blueprints/policy"))
    args = ap.parse_args()

    catalog = json.loads(args.catalog.read_text())
    blueprints = {bp["id"]: bp for bp in catalog.get("blueprints", [])}
    args.out.mkdir(parents=True, exist_ok=True)

    done, missing = [], []
    for bid in MATERIALIZED:
        bp = blueprints.get(bid)
        if bp is None:
            missing.append(bid)
            continue
        cands = list(args.src.glob(f"{bid}-*.spec.json"))
        if not cands:
            missing.append(bid)
            continue
        spec = json.loads(cands[0].read_text())
        injected = inject(spec, bp)
        out_path = args.out / f"swarm-{bid}{SUFFIX}.policy.json"
        out_path.write_text(json.dumps(injected, indent=2))
        done.append(bid)
    for bid in done:
        print(f"[ok] {bid}: policy injected -> {args.out / ('swarm-' + bid + SUFFIX + '.policy.json')}")
    if missing:
        print(f"[missing] {missing}", file=sys.stderr)
        return 2
    print(f"policy pass: {len(done)} blueprints")
    return 0


if __name__ == "__main__":
    sys.exit(main())
