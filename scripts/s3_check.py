#!/usr/bin/env python3
"""Check if a MinIO/S3 object exists using AWS Signature V4 via Python stdlib."""
import os, sys, hmac, hashlib, datetime, urllib.parse, urllib.request, urllib.error

ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SECRET = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
REGION = "us-east-1"

def sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

def get_signature_key(secret, date_stamp, region, service):
    k_date    = sign(("AWS4" + secret).encode("utf-8"), date_stamp)
    k_region  = sign(k_date, region)
    k_service = sign(k_region, service)
    k_signing = sign(k_service, "aws4_request")
    return k_signing

def s3_head(bucket, key):
    t = datetime.datetime.utcnow()
    amz_date   = t.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = t.strftime("%Y%m%d")
    host = urllib.parse.urlparse(ENDPOINT).netloc
    canonical_uri = f"/{bucket}/{key}"
    canonical_qs = ""
    canonical_headers = f"host:{host}\nx-amz-date:{amz_date}\n"
    signed_headers = "host;x-amz-date"
    payload_hash = hashlib.sha256(b"").hexdigest()
    canonical_request = "\n".join(["HEAD", canonical_uri, canonical_qs,
                                   canonical_headers, signed_headers, payload_hash])
    credential_scope = f"{date_stamp}/{REGION}/s3/aws4_request"
    string_to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date,
                                credential_scope,
                                hashlib.sha256(canonical_request.encode()).hexdigest()])
    signing_key = get_signature_key(SECRET, date_stamp, REGION, "s3")
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    auth = (f"AWS4-HMAC-SHA256 Credential={KEY}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}")
    req = urllib.request.Request(f"{ENDPOINT}/{bucket}/{key}", method="HEAD")
    req.add_header("x-amz-date", amz_date)
    req.add_header("Authorization", auth)
    try:
        urllib.request.urlopen(req, timeout=5)
        return True
    except urllib.error.HTTPError as e:
        return e.code == 200
    except Exception:
        return False

if __name__ == "__main__":
    bucket = sys.argv[1]          # e.g. "bronze"
    key    = sys.argv[2]          # e.g. "request/_delta_log/00000000000000000000.json"
    exists = s3_head(bucket, key)
    sys.exit(0 if exists else 1)
