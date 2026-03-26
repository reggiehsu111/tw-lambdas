"""
Example Lambda function for tw-lambdas.

Replace this file with your actual logic.
Entry point: lambda_handler(event, context)
"""

import json


def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Hello from tw-lambdas!",
            "event": event,
        }),
    }
