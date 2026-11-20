import json
from datetime import datetime, timezone

import boto3
import requests
from botocore.config import Config

API_URL = "https://dummyjson.com/products?limit=0"
S3_ENDPOINT = "http://localhost:4566"
BUCKET = "ucv-data-platform"


def get_products():
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("La respuesta de la API no es un objeto JSON.")

    if not isinstance(payload.get("products"), list):
        raise ValueError("La respuesta no contiene una lista 'products'.")

    if not payload["products"]:
        raise ValueError("La API devolvio una lista de productos vacia.")

    print(f"HTTP status: {response.status_code}")
    print(f"Products received: {len(payload['products'])}")

    return payload


def upload_bronze(payload):
    now = datetime.now(timezone.utc)
    ingestion_date = now.strftime("%Y-%m-%d")
    ingestion_ts = now.strftime("%Y%m%dT%H%M%S%fZ")

    key = (
        f"products/bronze/"
        f"ingestion_date={ingestion_date}/"
        f"products_{ingestion_ts}.json"
    )

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )

    body = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

    print(f"Bronze object created: s3://{BUCKET}/{key}")


if __name__ == "__main__":
    data = get_products()
    upload_bronze(data)