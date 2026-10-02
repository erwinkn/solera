"""What an S3-compatible bucket guarantees, checked (bench/railway): the
conditional writes Solera depends on — create-if-absent and compare-and-swap
— consistency, ranges, racing creates and multipart, raw and through obstore.

Reads ENDPOINT, BUCKET, ACCESS_KEY_ID, SECRET_ACCESS_KEY (and REGION) from
the environment; prints one JSON line per check, never a credential."""

from __future__ import annotations

import concurrent.futures
import json
import os
import sys
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT, BUCKET = os.environ["ENDPOINT"], os.environ["BUCKET"]
REGION = os.environ.get("REGION") or "auto"
PREFIX = f"probe-{uuid.uuid4().hex[:8]}/"
results: dict[str, bool] = {}


def report(name: str, ok: bool, **detail):
    results[name] = ok
    print("PROBE " + json.dumps({"check": name, "ok": ok, **detail}), flush=True)


def client(style: str, region: str = REGION):
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=os.environ["ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["SECRET_ACCESS_KEY"],
        region_name=region,
        config=Config(s3={"addressing_style": style}, retries={"max_attempts": 1}),
    )


def status(e: ClientError) -> int:
    return e.response["ResponseMetadata"]["HTTPStatusCode"]


def main() -> int:
    # Addressing and signing: what obstore's path-style, us-east-1 requests need.
    for style, region in (("virtual", REGION), ("path", REGION), ("path", "us-east-1")):
        try:
            client(style, region).put_object(Bucket=BUCKET, Key=f"{PREFIX}style", Body=b"x")
            report(f"put {style}-style, region {region}", True)
        except ClientError as e:
            report(
                f"put {style}-style, region {region}",
                False,
                status=status(e),
                code=e.response["Error"]["Code"],
            )
    s3 = client("path", "us-east-1") if results.get("put path-style, region us-east-1") else client("virtual")

    # 1. Create-if-absent: the second PUT with If-None-Match: * must be 412.
    key = f"{PREFIX}create"
    first = s3.put_object(Bucket=BUCKET, Key=key, Body=b"first", IfNoneMatch="*")
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"second", IfNoneMatch="*")
        report("if-none-match: second create refused", False, got="200")
    except ClientError as e:
        report("if-none-match: second create refused", status(e) == 412, status=status(e))
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    report("if-none-match: the first write stays", body == b"first")

    # 2. Compare-and-swap: If-Match with the current ETag succeeds, a stale one is 412.
    etag = first["ETag"]
    try:
        second = s3.put_object(Bucket=BUCKET, Key=key, Body=b"swapped", IfMatch=etag)
        report("if-match: current etag accepted", True)
    except ClientError as e:
        second = None
        report("if-match: current etag accepted", False, status=status(e))
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"stale", IfMatch=etag)
        report("if-match: stale etag refused", False, got="200")
    except ClientError as e:
        report("if-match: stale etag refused", status(e) == 412, status=status(e))
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    report("if-match: content is the swapped write", body == (b"swapped" if second else b"first"))

    # 3. Read-after-write and list-after-write, overwrite and delete included.
    fresh = [True, True, True, True]
    for i in range(50):
        k = f"{PREFIX}raw/{i:03d}"
        s3.put_object(Bucket=BUCKET, Key=k, Body=b"a%d" % i)
        fresh[0] &= s3.get_object(Bucket=BUCKET, Key=k)["Body"].read() == b"a%d" % i
        s3.put_object(Bucket=BUCKET, Key=k, Body=b"b%d" % i)
        fresh[1] &= s3.get_object(Bucket=BUCKET, Key=k)["Body"].read() == b"b%d" % i
        listed = {
            o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{PREFIX}raw/").get("Contents", [])
        }
        fresh[2] &= k in listed
    for i in range(50):
        k = f"{PREFIX}raw/{i:03d}"
        s3.delete_object(Bucket=BUCKET, Key=k)
        listed = {
            o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{PREFIX}raw/").get("Contents", [])
        }
        fresh[3] &= k not in listed
    report("read-after-write (new)", fresh[0])
    report("read-after-write (overwrite)", fresh[1])
    report("list-after-write", fresh[2])
    report("list-after-delete", fresh[3])

    # 4. Ranges.
    blob = bytes(range(256)) * 64
    s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}range", Body=blob)
    part = s3.get_object(Bucket=BUCKET, Key=f"{PREFIX}range", Range="bytes=1000-1099")["Body"].read()
    tail = s3.get_object(Bucket=BUCKET, Key=f"{PREFIX}range", Range="bytes=-48")["Body"].read()
    report("range get", part == blob[1000:1100] and tail == blob[-48:])

    # 5. Racing creates: N clients, one key, exactly one wins, its bytes stay.
    rounds, n, exact = 20, 24, 0
    pool = concurrent.futures.ThreadPoolExecutor(n)
    racers = [
        client("path", "us-east-1") if s3.meta.config.s3["addressing_style"] == "path" else client("virtual")
        for _ in range(n)
    ]
    for r in range(rounds):
        k = f"{PREFIX}race/{r}"

        def create(i, k=k):
            try:
                racers[i].put_object(Bucket=BUCKET, Key=k, Body=b"winner-%d" % i, IfNoneMatch="*")
                return i
            except ClientError as e:
                return None if status(e) in (409, 412) else ("error", status(e))

        outcomes = list(pool.map(create, range(n)))
        winners = [o for o in outcomes if isinstance(o, int)]
        errors = [o for o in outcomes if isinstance(o, tuple)]
        stays = (
            len(winners) == 1
            and s3.get_object(Bucket=BUCKET, Key=k)["Body"].read() == b"winner-%d" % winners[0]
        )
        exact += int(stays and not errors)
    report(
        "racing creates: exactly one winner", exact == rounds, rounds=rounds, clients=n, exact_rounds=exact
    )

    # 6. Multipart, and create-if-absent on completion.
    k = f"{PREFIX}multipart"
    up = s3.create_multipart_upload(Bucket=BUCKET, Key=k)["UploadId"]
    parts = []
    for i, size in enumerate((5 * 2**20, 1234), start=1):
        e = s3.upload_part(Bucket=BUCKET, Key=k, UploadId=up, PartNumber=i, Body=b"%d" % i * size)["ETag"]
        parts.append({"ETag": e, "PartNumber": i})
    s3.complete_multipart_upload(Bucket=BUCKET, Key=k, UploadId=up, MultipartUpload={"Parts": parts})
    size = s3.head_object(Bucket=BUCKET, Key=k)["ContentLength"]
    report("multipart upload", size == 5 * 2**20 + 1234)
    up = s3.create_multipart_upload(Bucket=BUCKET, Key=k)["UploadId"]
    e = s3.upload_part(Bucket=BUCKET, Key=k, UploadId=up, PartNumber=1, Body=b"z" * 100)["ETag"]
    try:
        s3.complete_multipart_upload(
            Bucket=BUCKET,
            Key=k,
            UploadId=up,
            MultipartUpload={"Parts": [{"ETag": e, "PartNumber": 1}]},
            IfNoneMatch="*",
        )
        report("multipart completion with if-none-match refused when present", False, got="200")
    except ClientError as err:
        report(
            "multipart completion with if-none-match refused when present",
            status(err) == 412,
            status=status(err),
        )

    # 7. obstore end to end: PutMode create and update, as Solera writes.
    import asyncio

    import obstore
    from obstore.store import S3Store

    path_style = s3.meta.config.s3["addressing_style"] == "path"
    store = S3Store(
        BUCKET,
        endpoint=ENDPOINT,
        access_key_id=os.environ["ACCESS_KEY_ID"],
        secret_access_key=os.environ["SECRET_ACCESS_KEY"],
        region="us-east-1" if path_style else REGION,
        virtual_hosted_style_request=not path_style,
    )

    async def obstore_checks():
        k = f"{PREFIX}obstore"
        put = await obstore.put_async(store, k, b"one", mode="create")
        try:
            await obstore.put_async(store, k, b"two", mode="create")
            report("obstore create: second refused", False)
        except Exception as e:
            report(
                "obstore create: second refused",
                type(e).__name__ == "AlreadyExistsError",
                error=type(e).__name__,
            )
        try:
            await obstore.put_async(store, k, b"three", mode={"e_tag": put["e_tag"]})
            report("obstore update: current etag accepted", True)
        except Exception as e:
            report("obstore update: current etag accepted", False, error=type(e).__name__)
        try:
            await obstore.put_async(store, k, b"four", mode={"e_tag": put["e_tag"]})
            report("obstore update: stale etag refused", False)
        except Exception as e:
            report(
                "obstore update: stale etag refused",
                type(e).__name__ == "PreconditionError",
                error=type(e).__name__,
            )
        got = await (await obstore.get_async(store, k)).bytes_async()
        report("obstore: content is the update", bytes(got) == b"three")

    asyncio.run(obstore_checks())

    # Clean up.
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get("Contents", [])]
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]})
    create_ok = (
        results["if-none-match: second create refused"] and results["racing creates: exactly one winner"]
    )
    print(
        "PROBE " + json.dumps({"summary": results, "create_if_absent": create_ok, "path_style": path_style}),
        flush=True,
    )
    return 0 if create_ok else 1


if __name__ == "__main__":
    sys.exit(main())
