"""Create the bucket on the local S3 server, upload the layer files, and configure toxiproxy.

Layer files are generated beforehand with head -c N /dev/urandom (see README in results.md).
"""

import json
import sys
import urllib.request
from pathlib import Path

import boto3

LAYER_DIR = Path(sys.argv[1])

s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="bench",
    aws_secret_access_key="benchbench",
    region_name="us-east-1",
)
existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
if "bench" not in existing:
    s3.create_bucket(Bucket="bench")
for f in sorted(LAYER_DIR.glob("*.bin")):
    s3.upload_file(str(f), "bench", f.name)
    print("uploaded", f.name, flush=True)


def toxi(path, body):
    req = urllib.request.Request(
        f"http://127.0.0.1:8474{path}", data=json.dumps(body).encode(), method="POST"
    )
    req.add_header("Content-Type", "application/json")
    return urllib.request.urlopen(req).read()


toxi("/proxies", {"name": "s3lat", "listen": "127.0.0.1:9101", "upstream": "127.0.0.1:9000"})
toxi(
    "/proxies/s3lat/toxics",
    {"name": "lat", "type": "latency", "stream": "upstream", "attributes": {"latency": 30}},
)
print("toxiproxy: 127.0.0.1:9101 -> 9000 with 30 ms upstream latency")
