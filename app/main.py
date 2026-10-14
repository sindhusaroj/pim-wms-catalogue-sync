"""Command-line entry point for running and operating the pipeline locally.

    python -m app.main run                      start a new run (local Step Functions stand-in)
    python -m app.main resume <run_id>          continue an interrupted run from its checkpoints
    python -m app.main status [<run_id>]        run status and reconciliation (latest runs if omitted)
    python -m app.main replay-dlq <run_id> [--include-ambiguous]
    python -m app.main start-execution          start the deployed Step Functions state machine and wait
"""
from __future__ import annotations

import argparse
import json
import sys

from app.config import Settings, get_settings
from app.observability import configure_logging


def start_execution(settings: Settings, timeout_seconds: int = 1800, poll_seconds: float = 5.0) -> dict:
    """Start the real state machine (AWS, or LocalStack with enable_compute = true) and wait for it."""
    import time

    from app.pipeline.deps import aws_client

    sfn = aws_client(settings, "stepfunctions")
    name = f"{settings.name_prefix}-run"
    machines = [m["stateMachineArn"] for m in sfn.list_state_machines()["stateMachines"] if m["name"] == name]
    if not machines:
        raise SystemExit(f"state machine {name} is not deployed (terraform: enable_compute = true)")
    execution = sfn.start_execution(stateMachineArn=machines[0], input="{}")["executionArn"]
    deadline = time.monotonic() + timeout_seconds
    while True:
        described = sfn.describe_execution(executionArn=execution)
        if described["status"] != "RUNNING" or time.monotonic() > deadline:
            break
        time.sleep(poll_seconds)
    output = json.loads(described["output"]) if described.get("output") else {}
    return {"execution": execution, "execution_status": described["status"], "error": described.get("error"),
            "cause": described.get("cause"), **output}


def _print(payload) -> None:
    print(json.dumps(payload, indent=2, default=str))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.main", description="PIM -> WMS catalogue synchronisation")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("run", help="start a new run")
    resume = sub.add_parser("resume", help="resume an interrupted run")
    resume.add_argument("run_id")
    status = sub.add_parser("status", help="show run status")
    status.add_argument("run_id", nargs="?")
    replay = sub.add_parser("replay-dlq", help="re-dispatch dead-lettered batches of a run")
    replay.add_argument("run_id")
    replay.add_argument("--include-ambiguous", action="store_true",
                        help="also replay AMBIGUOUS batches (only after confirming with the WMS team)")
    sub.add_parser("start-execution", help="start the deployed state machine and wait for it")
    args = parser.parse_args(argv)

    configure_logging()
    from app.local_orchestrator import run_pipeline, run_replay
    from app.pipeline.deps import build_deps
    from app.pipeline.finalize import summarize

    command = args.command or "run"
    if command == "start-execution":
        result = start_execution(get_settings())
        _print(result)
        return 0 if result["execution_status"] == "SUCCEEDED" and result.get("status") in ("COMPLETE", "NO_CHANGE") else 2

    deps = build_deps(get_settings())
    if command == "run":
        summary = run_pipeline(deps)
    elif command == "resume":
        summary = run_pipeline(deps, args.run_id, resume=True)
    elif command == "status":
        if not args.run_id:
            _print({"lock": deps.ledger.lock_holder(), "recent_runs": deps.ledger.recent_runs()})
            return 0
        summary = summarize(deps, args.run_id)
    else:
        summary = run_replay(deps, args.run_id, include_ambiguous=args.include_ambiguous)
    _print(summary)
    return 0 if summary.get("status") in ("COMPLETE", "NO_CHANGE", "RUNNING") else 2


if __name__ == "__main__":
    sys.exit(main())
