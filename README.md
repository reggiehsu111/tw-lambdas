# tw-lambdas

AWS Lambda functions for Taiwan stock and futures trading.

## Structure

```
tw-lambdas/
├── lambdas/
│   └── <function-name>/
│       ├── lambda_handler.py   ← entry point (required)
│       ├── config.json         ← function config (required)
│       ├── requirements.txt    ← pip deps (optional)
│       └── *.py                ← additional modules (optional)
├── shared/                     ← shared utilities (auto-bundled into every Lambda)
├── scripts/
│   └── deploy.py               ← generic deploy script
└── .gitignore
```

## Creating a New Lambda

1. Create a directory under `lambdas/`:
   ```
   lambdas/my-new-function/
   ```

2. Add `lambda_handler.py`:
   ```python
   def lambda_handler(event, context):
       return {"statusCode": 200, "body": "Hello!"}
   ```

3. Add `config.json`:
   ```json
   {
     "function_name": "my-new-function",
     "description": "What this does",
     "handler": "lambda_handler.lambda_handler",
     "runtime": "python3.11",
     "timeout": 60,
     "memory": 256,
     "env_vars": {
       "MY_VAR": "value"
     }
   }
   ```

4. (Optional) Add `requirements.txt` with pip dependencies.

5. Deploy:
   ```bash
   python scripts/deploy.py --function my-new-function
   ```

## Deploy Script

```bash
# Deploy a function
python scripts/deploy.py --function <name>

# Build zip only (no AWS deploy)
python scripts/deploy.py --function <name> --package-only
```

> **Note:** Always deploys using the `crypto_project` AWS profile (hardcoded).
> This uses the credentials defined in `~/.aws/credentials` under `[crypto_project]`.

## AWS Resources

- **Region:** `ap-northeast-1`
- **S3 Bucket:** `tw-lambdas-deployment` (auto-created on first deploy)
- **IAM Role:** `tw-lambdas-role` (auto-created on first deploy)
- **AWS Profile:** `crypto_project` (hardcoded — crypto credentials)
