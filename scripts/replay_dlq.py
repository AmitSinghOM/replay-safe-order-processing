#!/usr/bin/env python3
"""Auditable DLQ replay. Dry-run by default.

    python scripts/replay_dlq.py --stack replay-safe-orders --operator <alias> --reason "<why>"
    python scripts/replay_dlq.py --stack replay-safe-orders --operator <alias> --reason "<why>" --execute

Every message gets an AUDIT#replay row (operator, reason, message id, body
sha256, order status) BEFORE it is re-sent. Messages whose order already
settled are deleted from the DLQ without replay.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

import boto3  # noqa: E402

from orders.adapters.aws import DynamoStore, SqsDlq, SqsQueue  # noqa: E402
from orders.service import OrderService  # noqa: E402


def stack_outputs(stack: str) -> dict[str, str]:
    cfn = boto3.client("cloudformation")
    outs = cfn.describe_stacks(StackName=stack)["Stacks"][0].get("Outputs", [])
    return {o["OutputKey"]: o["OutputValue"] for o in outs}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--stack", required=True)
    p.add_argument("--operator", required=True)
    p.add_argument("--reason", required=True)
    p.add_argument("--max", type=int, default=10)
    p.add_argument("--execute", action="store_true", help="actually replay (default: dry run)")
    args = p.parse_args()

    out = stack_outputs(args.stack)
    service = OrderService(DynamoStore(out["TableName"]), SqsQueue(out["QueueUrl"]))
    report = service.replay_dlq(
        SqsDlq(out["DeadLetterQueueUrl"]),
        operator=args.operator,
        reason=args.reason,
        max_messages=args.max,
        dry_run=not args.execute,
    )
    print(json.dumps(report.__dict__, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
