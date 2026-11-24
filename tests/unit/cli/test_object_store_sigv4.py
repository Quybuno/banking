"""Known-answer test cho mã ký AWS Signature Version 4 của `n06.cli.object_store` (TASK T03b).

Nguồn vector: AWS Documentation, Amazon S3 API Reference, "Authenticating Requests: Using the
Authorization Header (AWS Signature Version 4)", mục "Examples: Signature Calculations":
  https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
Trang này hiện nạp nội dung bằng JavaScript nên không đọc được bằng HTTP thường. Các giá trị dưới
đây được trích từ văn bản trang trong bản chụp Internet Archive ngày 2022-01-17:
  https://web.archive.org/web/20220117222356/https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
(tải ngày 2026-11-24; trích bằng cách phân tích HTML, không viết từ trí nhớ). Mỗi ví dụ gồm
CanonicalRequest, giá trị SHA-256 của nó (dòng cuối của StringToSign), Signature và header
Authorization. Dữ liệu chung của các ví dụ: AWSAccessKeyId `AKIAIOSFODNN7EXAMPLE`,
AWSSecretAccessKey `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` (khóa ví dụ công khai của AWS,
không phải secret thật), thời điểm 20130524T000000Z, region us-east-1, bucket `examplebucket`.
"""

from __future__ import annotations

import hashlib
from typing import Any

import pytest

from n06.cli import doctor as doctor_module
from n06.cli.object_store import (
    BucketNameError,
    authorization_header,
    canonical_request,
    resolve_host_endpoint,
    signing_key,
    string_to_sign,
    validate_bucket_name,
)

ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
AMZ_DATE = "20130524T000000Z"
REGION = "us-east-1"
EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

# Mỗi vector: tên, tham số yêu cầu, các dòng KHÔNG rỗng của CanonicalRequest, hash trong
# StringToSign, Signature và Authorization, đều chép từ văn bản trang AWS.
VECTORS: list[dict[str, Any]] = [
    {
        "name": "GET Object (Range)",
        "method": "GET",
        "path": "/test.txt",
        "query": [],
        "headers": {
            "Host": "examplebucket.s3.amazonaws.com",
            "Range": "bytes=0-9",
            "x-amz-content-sha256": EMPTY_SHA256,
            "x-amz-date": AMZ_DATE,
        },
        "payload_sha256": EMPTY_SHA256,
        "canonical_lines": [
            "GET",
            "/test.txt",
            "host:examplebucket.s3.amazonaws.com",
            "range:bytes=0-9",
            "x-amz-content-sha256:" + EMPTY_SHA256,
            "x-amz-date:20130524T000000Z",
            "host;range;x-amz-content-sha256;x-amz-date",
            EMPTY_SHA256,
        ],
        "request_hash": "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972",
        "signature": "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
        "authorization": (
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
            "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,"
            "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        ),
    },
    {
        "name": "PUT Object (test$file.text)",
        "method": "PUT",
        "path": "/test$file.text",
        "query": [],
        "headers": {
            "Date": "Fri, 24 May 2013 00:00:00 GMT",
            "Host": "examplebucket.s3.amazonaws.com",
            "x-amz-date": AMZ_DATE,
            "x-amz-storage-class": "REDUCED_REDUNDANCY",
            "x-amz-content-sha256": (
                "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
            ),
        },
        "payload_sha256": "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
        "canonical_lines": [
            "PUT",
            "/test%24file.text",
            "date:Fri, 24 May 2013 00:00:00 GMT",
            "host:examplebucket.s3.amazonaws.com",
            "x-amz-content-sha256:44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
            "x-amz-date:20130524T000000Z",
            "x-amz-storage-class:REDUCED_REDUNDANCY",
            "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class",
            "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
        ],
        "request_hash": "9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d",
        "signature": "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
        "authorization": (
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
            "SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,"
            "Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        ),
    },
    {
        "name": "GET Bucket Lifecycle (?lifecycle)",
        "method": "GET",
        "path": "/",
        "query": [("lifecycle", "")],
        "headers": {
            "Host": "examplebucket.s3.amazonaws.com",
            "x-amz-date": AMZ_DATE,
            "x-amz-content-sha256": EMPTY_SHA256,
        },
        "payload_sha256": EMPTY_SHA256,
        "canonical_lines": [
            "GET",
            "/",
            "lifecycle=",
            "host:examplebucket.s3.amazonaws.com",
            "x-amz-content-sha256:" + EMPTY_SHA256,
            "x-amz-date:20130524T000000Z",
            "host;x-amz-content-sha256;x-amz-date",
            EMPTY_SHA256,
        ],
        "request_hash": "9766c798316ff2757b517bc739a67f6213b4ab36dd5da2f94eaebf79c77395ca",
        "signature": "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
        "authorization": (
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            "Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"
        ),
    },
    {
        "name": "GET Bucket (List Objects, max-keys=2&prefix=J)",
        "method": "GET",
        "path": "/",
        # Truyền theo thứ tự đảo để kiểm tra việc sắp xếp query.
        "query": [("prefix", "J"), ("max-keys", "2")],
        "headers": {
            "Host": "examplebucket.s3.amazonaws.com",
            "x-amz-date": AMZ_DATE,
            "x-amz-content-sha256": EMPTY_SHA256,
        },
        "payload_sha256": EMPTY_SHA256,
        "canonical_lines": [
            "GET",
            "/",
            "max-keys=2&prefix=J",
            "host:examplebucket.s3.amazonaws.com",
            "x-amz-content-sha256:" + EMPTY_SHA256,
            "x-amz-date:20130524T000000Z",
            "host;x-amz-content-sha256;x-amz-date",
            EMPTY_SHA256,
        ],
        "request_hash": "df57d21db20da04d7fa30298dd4488ba3a2b47ca3a489c74750e0f1e7df1b9b7",
        "signature": "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
        "authorization": (
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
            "Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
        ),
    },
]


def _ids() -> list[str]:
    return [str(v["name"]) for v in VECTORS]


@pytest.mark.parametrize("vector", VECTORS, ids=_ids())
def test_canonical_request_and_string_to_sign_match_aws_documentation(
    vector: dict[str, Any],
) -> None:
    text, _signed = canonical_request(
        method=vector["method"],
        path=vector["path"],
        query=vector["query"],
        headers=vector["headers"],
        payload_sha256=vector["payload_sha256"],
    )
    # Các dòng không rỗng khớp văn bản CanonicalRequest của AWS (dòng rỗng của query và của
    # khối header được bỏ khỏi so sánh, nhưng được kiểm bằng hash bên dưới).
    assert [line for line in text.split("\n") if line] == vector["canonical_lines"]

    to_sign = string_to_sign(amz_date=AMZ_DATE, region=REGION, canonical_request_text=text)
    assert to_sign.split("\n") == [
        "AWS4-HMAC-SHA256",
        AMZ_DATE,
        "20130524/us-east-1/s3/aws4_request",
        vector["request_hash"],
    ]
    # Hash đúng từng byte chứng tỏ cả các dòng rỗng của CanonicalRequest cũng đúng.
    assert hashlib.sha256(text.encode("utf-8")).hexdigest() == vector["request_hash"]


@pytest.mark.parametrize("vector", VECTORS, ids=_ids())
def test_signature_and_authorization_header_match_aws_documentation(
    vector: dict[str, Any],
) -> None:
    header = authorization_header(
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        region=REGION,
        amz_date=AMZ_DATE,
        method=vector["method"],
        path=vector["path"],
        query=vector["query"],
        headers=vector["headers"],
        payload_sha256=vector["payload_sha256"],
    )
    assert header == vector["authorization"]
    assert header.endswith("Signature=" + vector["signature"])


def test_put_example_payload_hash_is_hash_of_documented_body() -> None:
    # Ví dụ PUT của AWS: nội dung tải lên là chuỗi "Welcome to Amazon S3."
    assert (
        hashlib.sha256(b"Welcome to Amazon S3.").hexdigest()
        == "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"
    )


def test_signing_key_depends_on_date_region_and_secret() -> None:
    base = signing_key(secret_key=SECRET_KEY, amz_date=AMZ_DATE, region=REGION)
    assert len(base) == 32
    assert base != signing_key(secret_key=SECRET_KEY, amz_date="20130525T000000Z", region=REGION)
    assert base != signing_key(secret_key=SECRET_KEY, amz_date=AMZ_DATE, region="eu-west-1")
    assert base != signing_key(secret_key=SECRET_KEY + "x", amz_date=AMZ_DATE, region=REGION)


@pytest.mark.parametrize("name", ["n06-lakehouse", "n06-benchmark-lakehouse", "abc", "a1b"])
def test_valid_bucket_names_are_accepted(name: str) -> None:
    assert validate_bucket_name(name) == name


@pytest.mark.parametrize(
    "name",
    ["", "ab", "N06-lakehouse", "n06_lakehouse", "-abc", "abc-", "a.b.c", "a b", "x" * 64, "a/b"],
)
def test_invalid_bucket_names_are_rejected(name: str) -> None:
    with pytest.raises(BucketNameError):
        validate_bucket_name(name)


def test_resolve_host_endpoint_matches_what_doctor_probes() -> None:
    """Địa chỉ host của object store phải trùng địa chỉ mà `doctor` dùng cho service minio."""
    env = {
        "N06_OBJECT_STORE_ACCESS_KEY": "a",
        "N06_OBJECT_STORE_SECRET_KEY": "b",
        "N06_POSTGRES_MART_PASSWORD": "c",
        "N06_POSTGRES_CONTROL_PASSWORD": "d",
        "N06_POSTGRES_AIRFLOW_PASSWORD": "e",
        "N06_POSTGRES_METABASE_PASSWORD": "f",
        "N06_MINIO_API_HOST_PORT": "29000",
    }
    scheme, host, port = resolve_host_endpoint(env)
    doctor_host, doctor_port, health_url = doctor_module._resolve_host_endpoints(env)["minio"]
    assert (host, port) == (doctor_host, doctor_port) == ("localhost", 29000)
    assert health_url == f"{scheme}://{host}:{port}/minio/health/live"


def test_target_repr_never_contains_secret_key() -> None:
    from n06.cli.object_store import ObjectStoreTarget

    target = ObjectStoreTarget("http", "h", 1, REGION, ACCESS_KEY, SECRET_KEY)
    assert SECRET_KEY not in repr(target)
