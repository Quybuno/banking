"""Known-answer test cho bộ ký SigV4 đã dời sang `n06.storage.object_store` (ADR-0012 quyết định 2).

Dùng lại vector `PUT Object (test$file.text)` của AWS S3 API Reference
(thân `Welcome to Amazon S3.`) từ `tests/unit/cli/test_object_store_sigv4.py`.
"""

from __future__ import annotations

import hashlib

from n06.storage.object_store import (
    authorization_header,
    canonical_request,
    signing_key,
    string_to_sign,
)

ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
AMZ_DATE = "20130524T000000Z"
REGION = "us-east-1"


def test_storage_sigv4_put_object_known_answer_vector() -> None:
    payload = b"Welcome to Amazon S3."
    payload_sha256 = hashlib.sha256(payload).hexdigest()
    assert payload_sha256 == "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"

    headers = {
        "Date": "Fri, 24 May 2013 00:00:00 GMT",
        "Host": "examplebucket.s3.amazonaws.com",
        "x-amz-date": AMZ_DATE,
        "x-amz-storage-class": "REDUCED_REDUNDANCY",
        "x-amz-content-sha256": payload_sha256,
    }
    request_text, signed_headers = canonical_request(
        method="PUT",
        path="/test$file.text",
        query=(),
        headers=headers,
        payload_sha256=payload_sha256,
    )
    assert signed_headers == "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class"
    assert (
        hashlib.sha256(request_text.encode("utf-8")).hexdigest()
        == "9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d"
    )
    to_sign = string_to_sign(amz_date=AMZ_DATE, region=REGION, canonical_request_text=request_text)
    assert to_sign.endswith("9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d")
    assert len(signing_key(secret_key=SECRET_KEY, amz_date=AMZ_DATE, region=REGION)) == 32

    auth = authorization_header(
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        region=REGION,
        amz_date=AMZ_DATE,
        method="PUT",
        path="/test$file.text",
        query=(),
        headers=headers,
        payload_sha256=payload_sha256,
    )
    assert auth == (
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
        "SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,"
        "Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
    )
