from pathlib import Path

import boto3


def join_path(base: str, *parts: str) -> str:
    """Join path parts for local or S3 URIs."""
    if base.startswith("s3://"):
        return "/".join([base.rstrip("/")] + list(parts))
    return str(Path(base, *parts))


def model_exists(path: str) -> bool:
    """Check if the given path exists on S3 or locally."""
    if path.startswith("s3://"):
        bucket, key = path.replace("s3://", "", 1).split("/", 1)
        s3_client = boto3.client("s3")
        response = s3_client.list_objects(Bucket=bucket, MaxKeys=1, Prefix=key)
        return "Contents" in response
    return Path(path).exists()
