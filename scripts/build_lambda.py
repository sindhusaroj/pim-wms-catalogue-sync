"""Build build/lambda.zip: the app package plus its Linux wheels for the Lambda runtime.

    python scripts/build_lambda.py                 # arm64 (production default)
    python scripts/build_lambda.py --arch x86_64   # e.g. LocalStack on an x86 host

boto3 is provided by the Lambda runtime and is not bundled.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / "build"
RUNTIME_DEPS = ["httpx==0.28.1", "pydantic==2.12.5"]
PLATFORMS = {"arm64": "manylinux2014_aarch64", "x86_64": "manylinux2014_x86_64"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--arch", choices=sorted(PLATFORMS), default="arm64")
    parser.add_argument("--python-version", default="3.12")
    args = parser.parse_args()

    staging = BUILD / "lambda"
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--quiet", "--target", str(staging), "--platform", PLATFORMS[args.arch],
         "--implementation", "cp", "--python-version", args.python_version, "--only-binary=:all:", *RUNTIME_DEPS],
        check=True,
    )
    shutil.copytree(ROOT / "app", staging / "app", ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    archive = shutil.make_archive(str(BUILD / "lambda"), "zip", staging)
    print(f"built {archive} ({Path(archive).stat().st_size // 1024} KiB) for {args.arch}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
