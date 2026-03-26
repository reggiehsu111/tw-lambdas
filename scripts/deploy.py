#!/usr/bin/env python3
"""
Generic Lambda deploy script for tw-lambdas.

Usage:
    python scripts/deploy.py --function <lambda-dir-name>
    python scripts/deploy.py --function tw-example --package-only

Each Lambda lives in lambdas/<name>/ and must contain:
  - lambda_handler.py     (entry point, must define lambda_handler(event, context))
  - config.json           (function config: name, timeout, memory, env_vars, etc.)
  - requirements.txt      (optional: pip dependencies)

AWS credentials: hardcoded to 'crypto_project' profile in ~/.aws/credentials
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# ─── HARDCODED: always use crypto_project AWS credentials ──────────────────────
AWS_PROFILE = "crypto_project"
AWS_REGION = "ap-northeast-1"
DEPLOYMENT_BUCKET = "tw-lambdas-deployment"
LAMBDA_ROLE_NAME = "tw-lambdas-role"
# ───────────────────────────────────────────────────────────────────────────────

SCRIPTS_DIR = Path(__file__).parent
REPO_ROOT = SCRIPTS_DIR.parent
LAMBDAS_DIR = REPO_ROOT / "lambdas"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Deploy a Lambda function from lambdas/<name>/"
    )
    parser.add_argument(
        "--function", "-f",
        required=True,
        help="Lambda directory name under lambdas/ (e.g. tw-example)",
    )
    parser.add_argument(
        "--package-only",
        action="store_true",
        help="Build zip only, skip AWS deployment",
    )
    return parser.parse_args()


def load_config(lambda_dir: Path) -> dict:
    config_path = lambda_dir / "config.json"
    if not config_path.exists():
        print(f"❌ config.json not found in {lambda_dir}")
        sys.exit(1)
    with open(config_path) as f:
        return json.load(f)


def run_command(command: str | list, cwd: Path | None = None) -> str:
    result = subprocess.run(
        command,
        shell=isinstance(command, str),
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if result.returncode != 0:
        print(f"❌ Command failed:\n{result.stderr}")
        sys.exit(1)
    return result.stdout


def build_package(lambda_dir: Path, config: dict) -> Path:
    """Package lambda_handler.py + local .py files + requirements into a zip."""
    function_name = config["function_name"]
    build_dir = REPO_ROOT / f".build_{function_name}"
    zip_path = REPO_ROOT / f"{function_name}.zip"

    print(f"\n📦 Building package for {function_name}...")

    # Clean up
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir()

    # Copy all .py files from lambda dir
    py_files = list(lambda_dir.glob("*.py"))
    if not (lambda_dir / "lambda_handler.py").exists():
        print(f"❌ lambda_handler.py not found in {lambda_dir}")
        sys.exit(1)

    for py_file in py_files:
        shutil.copy(py_file, build_dir / py_file.name)
        print(f"  Copied {py_file.name}")

    # Copy shared/ utilities if they exist
    shared_dir = REPO_ROOT / "shared"
    if shared_dir.exists():
        shutil.copytree(shared_dir, build_dir / "shared")
        print(f"  Copied shared/")

    # Install requirements (Linux x86_64 compatible)
    req_file = lambda_dir / "requirements.txt"
    if req_file.exists() and req_file.stat().st_size > 0:
        print(f"  Installing dependencies...")
        run_command(
            f"{sys.executable} -m pip install "
            f"-r {req_file} "
            f"--platform manylinux2014_x86_64 "
            f"--implementation cp "
            f"--python-version 3.11 "
            f"--only-binary=:all: "
            f"--target {build_dir} "
            f"--quiet"
        )
        print(f"  ✅ Dependencies installed")
    else:
        print(f"  No requirements.txt (skipping pip install)")

    # Zip it
    if zip_path.exists():
        zip_path.unlink()

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(build_dir):
            for file in files:
                full = Path(root) / file
                arcname = full.relative_to(build_dir)
                zf.write(full, arcname)

    size_mb = zip_path.stat().st_size / (1024 * 1024)
    print(f"  ✅ Package: {zip_path.name} ({size_mb:.1f} MB)")

    # Warn if too large
    if size_mb > 50:
        print(f"  ⚠️  Package is large ({size_mb:.1f} MB). Consider using S3 upload.")
    if size_mb > 250:
        print(f"  ❌ Package exceeds Lambda's 250 MB unzipped limit!")
        sys.exit(1)

    # Cleanup build dir
    shutil.rmtree(build_dir)

    return zip_path


def get_or_create_session() -> boto3.Session:
    """Create boto3 session using hardcoded crypto_project profile."""
    session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    try:
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        print(f"\n🔑 AWS Identity:")
        print(f"   Profile:    {AWS_PROFILE}  ← (hardcoded, crypto credentials)")
        print(f"   Account ID: {identity['Account']}")
        print(f"   ARN:        {identity['Arn']}")
        return session
    except Exception as e:
        print(f"❌ Could not authenticate with profile '{AWS_PROFILE}': {e}")
        print(f"   Make sure [{AWS_PROFILE}] exists in ~/.aws/credentials")
        sys.exit(1)


def ensure_s3_bucket(session: boto3.Session) -> None:
    s3 = session.client("s3")
    try:
        s3.head_bucket(Bucket=DEPLOYMENT_BUCKET)
        print(f"  S3 bucket exists: {DEPLOYMENT_BUCKET}")
    except ClientError:
        print(f"  Creating S3 bucket: {DEPLOYMENT_BUCKET}")
        s3.create_bucket(
            Bucket=DEPLOYMENT_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
        )
        print(f"  ✅ S3 bucket created")


def get_or_create_role(session: boto3.Session) -> str:
    iam = session.client("iam")
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }
    try:
        role = iam.get_role(RoleName=LAMBDA_ROLE_NAME)
        role_arn = role["Role"]["Arn"]
        print(f"  IAM role: {role_arn}")
        return role_arn
    except iam.exceptions.NoSuchEntityException:
        print(f"  Creating IAM role: {LAMBDA_ROLE_NAME}")
        response = iam.create_role(
            RoleName=LAMBDA_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for tw-lambdas Lambda functions",
        )
        role_arn = response["Role"]["Arn"]
        for policy_arn in [
            "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
            "arn:aws:iam::aws:policy/AmazonS3FullAccess",
        ]:
            iam.attach_role_policy(RoleName=LAMBDA_ROLE_NAME, PolicyArn=policy_arn)
        print(f"  ✅ IAM role created: {role_arn}")
        print(f"  Waiting for IAM role to propagate...")
        time.sleep(10)
        return role_arn


def wait_for_lambda_active(lam, function_name: str, max_attempts: int = 30):
    print(f"  Waiting for Lambda to become active...", end="", flush=True)
    for _ in range(max_attempts):
        try:
            resp = lam.get_function(FunctionName=function_name)
            state = resp["Configuration"]["State"]
            last_status = resp["Configuration"].get("LastUpdateStatus", "Successful")
            if state == "Active" and last_status == "Successful":
                print(" ✅")
                return
            elif state == "Failed" or last_status == "Failed":
                print(" ❌")
                raise Exception(f"Lambda failed: {resp['Configuration'].get('StateReason')}")
        except lam.exceptions.ResourceNotFoundException:
            pass
        print(".", end="", flush=True)
        time.sleep(3)
    raise Exception("Lambda did not become active in time")


def deploy(session: boto3.Session, config: dict, zip_path: Path, role_arn: str):
    """Upload zip to S3 and create/update the Lambda function."""
    lam = session.client("lambda")
    s3 = session.client("s3")

    function_name = config["function_name"]
    handler = config.get("handler", "lambda_handler.lambda_handler")
    runtime = config.get("runtime", "python3.11")
    timeout = config.get("timeout", 60)
    memory = config.get("memory", 256)
    env_vars = config.get("env_vars", {})
    description = config.get("description", f"tw-lambdas: {function_name}")

    # Upload to S3
    s3_key = f"packages/{function_name}/{zip_path.name}"
    print(f"\n⬆️  Uploading to s3://{DEPLOYMENT_BUCKET}/{s3_key}")
    s3.upload_file(str(zip_path), DEPLOYMENT_BUCKET, s3_key)
    print(f"  ✅ Upload complete")

    print(f"\n🚀 Deploying Lambda: {function_name}")

    try:
        lam.update_function_code(
            FunctionName=function_name,
            S3Bucket=DEPLOYMENT_BUCKET,
            S3Key=s3_key,
        )
        print(f"  Updated function code")
        wait_for_lambda_active(lam, function_name)
        lam.update_function_configuration(
            FunctionName=function_name,
            Handler=handler,
            Runtime=runtime,
            Timeout=timeout,
            MemorySize=memory,
            Environment={"Variables": env_vars},
            Description=description,
        )
        print(f"  ✅ Function updated")

    except lam.exceptions.ResourceNotFoundException:
        print(f"  Creating new Lambda function...")
        lam.create_function(
            FunctionName=function_name,
            Runtime=runtime,
            Role=role_arn,
            Handler=handler,
            Code={"S3Bucket": DEPLOYMENT_BUCKET, "S3Key": s3_key},
            Timeout=timeout,
            MemorySize=memory,
            Environment={"Variables": env_vars},
            Description=description,
        )
        wait_for_lambda_active(lam, function_name)
        print(f"  ✅ Function created")

    # Print summary
    fn = lam.get_function(FunctionName=function_name)["Configuration"]
    print(f"\n{'=' * 50}")
    print(f"✅ Deployment complete!")
    print(f"   Function: {fn['FunctionName']}")
    print(f"   ARN:      {fn['FunctionArn']}")
    print(f"   Runtime:  {fn['Runtime']}")
    print(f"   Timeout:  {fn['Timeout']}s")
    print(f"   Memory:   {fn['MemorySize']} MB")
    print(f"{'=' * 50}")


def setup_eventbridge_schedule(session: boto3.Session, function_name: str, schedule_path: Path) -> None:
    """Set up EventBridge schedule from schedule.json if present."""
    if not schedule_path.exists():
        return

    with open(schedule_path) as f:
        sched = json.load(f)

    if not sched.get("enabled", False):
        print(f"  schedule.json found but enabled=false, skipping")
        return

    expression = sched["expression"]
    description = sched.get("description", f"Schedule for {function_name}")

    events = session.client("events")
    lam = session.client("lambda")
    sts = session.client("sts")
    account_id = sts.get_caller_identity()["Account"]

    rule_name = f"{function_name}-schedule"
    lambda_arn = f"arn:aws:lambda:{AWS_REGION}:{account_id}:function:{function_name}"

    print(f"\n⏰ Setting up EventBridge schedule: {expression}")

    # Create/update rule
    response = events.put_rule(
        Name=rule_name,
        ScheduleExpression=expression,
        State="ENABLED",
        Description=description,
    )
    rule_arn = response["RuleArn"]

    # Add Lambda as target
    events.put_targets(
        Rule=rule_name,
        Targets=[{"Id": "1", "Arn": lambda_arn}],
    )

    # Grant EventBridge permission to invoke Lambda
    try:
        lam.remove_permission(FunctionName=function_name, StatementId="EventBridgeInvoke")
    except lam.exceptions.ResourceNotFoundException:
        pass
    lam.add_permission(
        FunctionName=function_name,
        StatementId="EventBridgeInvoke",
        Action="lambda:InvokeFunction",
        Principal="events.amazonaws.com",
        SourceArn=rule_arn,
    )
    print(f"  ✅ Schedule set: {expression}")
    print(f"     Description: {description}")


def main():
    args = parse_args()

    lambda_dir = LAMBDAS_DIR / args.function
    if not lambda_dir.exists():
        print(f"❌ Lambda directory not found: {lambda_dir}")
        print(f"   Available: {[d.name for d in LAMBDAS_DIR.iterdir() if d.is_dir()]}")
        sys.exit(1)

    config = load_config(lambda_dir)

    print("=" * 50)
    print(f"🇹🇼 tw-lambdas deploy")
    print(f"   Function dir: lambdas/{args.function}/")
    print(f"   Lambda name:  {config['function_name']}")
    print(f"   Region:       {AWS_REGION}")
    print(f"   AWS Profile:  {AWS_PROFILE}  ← crypto credentials (hardcoded)")
    print("=" * 50)

    zip_path = build_package(lambda_dir, config)

    if args.package_only:
        print(f"\n--package-only: skipping deploy. Zip at: {zip_path}")
        return

    session = get_or_create_session()

    print(f"\n🔧 Setting up AWS resources...")
    ensure_s3_bucket(session)
    role_arn = get_or_create_role(session)

    deploy(session, config, zip_path, role_arn)

    # EventBridge schedule (optional)
    setup_eventbridge_schedule(session, config["function_name"], lambda_dir / "schedule.json")

    # Cleanup zip
    zip_path.unlink()
    print(f"  Cleaned up {zip_path.name}")


if __name__ == "__main__":
    main()
