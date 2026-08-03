from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


class MinioStorage:
    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        region_name: str = "us-east-1",
    ) -> None:
        self.bucket_name = bucket_name

        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
            config=Config(signature_version="s3v4"),
        )

    def ensure_bucket_exists(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code")

            if error_code in {"404", "NoSuchBucket", "NotFound"}:
                self.client.create_bucket(Bucket=self.bucket_name)
                return

            raise

    def put_json(
        self,
        object_key: str,
        payload: dict[str, Any],
    ) -> None:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8")

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=object_key,
            Body=body,
            ContentType="application/json",
        )

    def list_json_objects(self, prefix: str) -> list[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        object_keys: list[str] = []

        for page in paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=prefix,
        ):
            for item in page.get("Contents", []):
                object_key = item["Key"]

                if object_key.endswith(".json"):
                    object_keys.append(object_key)

        return object_keys

    def get_json(
        self,
        object_key: str,
    ) -> dict[str, Any]:
        response = self.client.get_object(
            Bucket=self.bucket_name,
            Key=object_key,
        )

        body = response["Body"].read()

        return json.loads(body)