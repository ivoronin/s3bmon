"""AWS API calls for S3 Batch Operations monitoring"""

import aioboto3

session = aioboto3.Session()


async def get_account_id():
    """Get the AWS account ID"""
    async with session.client("sts") as sts:
        identity = await sts.get_caller_identity()
        return identity["Account"]


async def list_jobs(account_id):
    """Get all S3 Batch Operations jobs"""
    next_token = None

    jobs = []

    async with session.client("s3control") as s3control:
        while True:
            if next_token:
                response = await s3control.list_jobs(
                    AccountId=account_id, NextToken=next_token
                )
            else:
                response = await s3control.list_jobs(AccountId=account_id)

            jobs.extend(response["Jobs"])

            next_token = response.get("NextToken")
            if not next_token:
                break

    return jobs


async def describe_job(account_id, job_id):
    """Get the details of an S3 Batch Operations job"""
    async with session.client("s3control") as s3control:
        response = await s3control.describe_job(AccountId=account_id, JobId=job_id)
        return response["Job"]
