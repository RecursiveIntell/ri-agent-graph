#!/usr/bin/env python3
"""Authenticated operator client: decide a durable checkpoint-bound approval.

Speaks agent_graph.operator.v1 over the daemon's peer-credentialed operator socket.
Auth = kernel peer credentials (same-uid client) + nonce + time window + frame validity.

Usage:
  operator_decide.py --approval-id <id> --decision approve|reject [--checkpoint-digest <digest>]
                     [--socket <path>] [--actor <label>]
"""
import argparse
import json
import socket
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--approval-id", required=True)
    ap.add_argument("--decision", required=True, choices=["approve", "reject"])
    ap.add_argument("--checkpoint-digest", default="")
    ap.add_argument("--socket", default="/home/sikmindz/.local/share/agent-graph/run/operator.sock")
    ap.add_argument("--actor", default="operator")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    frame = {
        "protocol": "agent_graph.operator.v1",
        "request_id": f"op-{uuid.uuid4().hex[:16]}",
        "action": "decide_approval",
        "resource_kind": "approval",
        "resource_id": args.approval_id,
        "expected_state_digest": args.checkpoint_digest,
        "nonce": uuid.uuid4().hex,
        "issued_at": (now - timedelta(seconds=1)).isoformat(),
        "expires_at": (now + timedelta(minutes=1)).isoformat(),
        "decision_material": json.dumps(
            {"decision": args.decision, "claimed_actor_label": args.actor}
        ),
    }
    body = json.dumps(frame).encode()
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(10)
        s.connect(args.socket)
    except OSError as e:
        print(f"operator socket connect failed: {e}", file=sys.stderr)
        return 2
    s.sendall(len(body).to_bytes(4, "big") + body)
    header = s.recv(4)
    if len(header) != 4:
        print("operator socket closed before response", file=sys.stderr)
        return 3
    length = int.from_bytes(header, "big")
    payload = b""
    while len(payload) < length:
        chunk = s.recv(length - len(payload))
        if not chunk:
            break
        payload += chunk
    s.close()
    resp = json.loads(payload.decode())
    print(json.dumps(resp, indent=2))
    return 0 if resp.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
