#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "psycopg2-binary>=2.9",
#   "minio>=7.0",
#   "pymongo>=4.0,<4.11",
# ]
# ///
"""E2E: mongo-oplog source -> S3 Parquet sink via MinIO.

Validates the full pipeline: MongoDB oplog CDC -> RisingWave streaming -> S3 Parquet sink.
Uses MinIO (RisingWave's S3-compatible backend) and a MongoDB 3.6 replica set (via mup).

Prerequisites:
  - RisingWave running with MinIO: ./risedev d for-ctl
  - MongoDB 3.6 RS running: mup playground start --version 3.6
  - uv (https://docs.astral.sh/uv/) — deps are managed inline via PEP 723

Run: uv run e2e_test/s3/mongo_oplog_parquet_sink.py
"""

import os
import subprocess
from time import sleep

import psycopg2
import pymongo
from minio import Minio

# --- Config (overridable via env vars) ---
RW_HOST = os.environ.get("RW_HOST", "localhost")
RW_PORT = os.environ.get("RW_PORT", "4566")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "127.0.0.1:9301")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS", "hummockadmin")
MINIO_SECRET = os.environ.get("MINIO_SECRET", "hummockadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "hummock001")
SINK_PATH = "mongo_oplog_parquet_test/"
MONGO_DB = "e2edb"
MONGO_COLL = "events"
NUM_DOCS = 20
MAX_RETRIES = 30
RETRY_INTERVAL_SECS = 5


def get_mongo_uri():
    """Get MongoDB connection URI from env or mup playground status.

    Returns the standard multi-host replica set URI. pymongo uses it to
    auto-discover the primary for writes; the RW connector uses it to
    connect to the RS and select a member via its own read preference.
    """
    env_uri = os.environ.get("MONGO_URI")
    if env_uri:
        return env_uri

    # Parse from mup playground status output
    try:
        result = subprocess.run(
            ["mup", "playground", "status"],
            capture_output=True, text=True, timeout=10
        )
        for line in result.stdout.splitlines():
            stripped = line.strip()
            if stripped.startswith("mongodb://"):
                return stripped
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    # Fallback to mup default ports
    return "mongodb://localhost:30000/?replicaSet=rs0"


def seed_mongodb(mongo_uri):
    """Insert seed documents into MongoDB collection."""
    print(f"Connecting to MongoDB: {mongo_uri}")
    client = pymongo.MongoClient(mongo_uri)
    coll = client[MONGO_DB][MONGO_COLL]
    coll.drop()
    docs = [{"seq": i, "name": f"event_{i}", "value": i * 10} for i in range(NUM_DOCS)]
    coll.insert_many(docs, bypass_document_validation=True)
    print(f"Inserted {NUM_DOCS} seed documents into {MONGO_DB}.{MONGO_COLL}")
    client.close()


def create_rw_pipeline(cur, mongo_uri):
    """Create RisingWave CDC table and S3 Parquet sink."""

    # CDC table: DEBEZIUM_MONGO requires CREATE TABLE with (_id, payload) columns
    print("Creating mongo-oplog CDC table...")
    cur.execute(f"""
    CREATE TABLE mongo_e2e_source (
        _id VARCHAR PRIMARY KEY,
        payload JSONB
    ) WITH (
        connector = 'mongo-oplog',
        mongodb.url = '{mongo_uri}',
        mongodb.namespace = '{MONGO_DB}.{MONGO_COLL}'
    ) FORMAT DEBEZIUM_MONGO ENCODE JSON;
    """)

    # S3 Parquet sink directly from the CDC table
    print("Creating S3 Parquet sink...")
    cur.execute(f"""
    CREATE SINK mongo_e2e_parquet_sink AS SELECT * FROM mongo_e2e_source WITH (
        connector = 's3',
        s3.region_name = 'custom',
        s3.bucket_name = '{MINIO_BUCKET}',
        s3.credentials.access = '{MINIO_ACCESS}',
        s3.credentials.secret = '{MINIO_SECRET}',
        s3.endpoint_url = 'http://{MINIO_BUCKET}.{MINIO_ENDPOINT}',
        s3.path = '{SINK_PATH}',
        type = 'append-only',
        force_append_only = 'true',
        rollover_seconds = 5
    ) FORMAT PLAIN ENCODE PARQUET(force_append_only='true');
    """)
    print("RisingWave pipeline created.")


def wait_for_parquet_files(minio_client):
    """Wait for Parquet files to appear in MinIO bucket."""
    parquet_files = []
    for attempt in range(MAX_RETRIES):
        objects = list(minio_client.list_objects(
            MINIO_BUCKET, prefix=SINK_PATH, recursive=True
        ))
        parquet_files = [o.object_name for o in objects if o.object_name.endswith(".parquet")]
        if parquet_files:
            break
        print(f"[retry {attempt}] No parquet files yet, waiting {RETRY_INTERVAL_SECS}s...")
        sleep(RETRY_INTERVAL_SECS)

    assert len(parquet_files) > 0, "Expected at least one .parquet file in MinIO"
    print(f"Found {len(parquet_files)} parquet file(s):")
    for f in parquet_files:
        print(f"  {f}")
    return parquet_files


def verify_row_count(cur):
    """Create a table that reads sink parquet output back and verify row count."""
    print("Creating verification table from parquet files...")
    cur.execute(f"""
    CREATE TABLE mongo_e2e_verify (
        _id VARCHAR,
        payload VARCHAR
    ) WITH (
        connector = 's3',
        match_pattern = '{SINK_PATH}*.parquet',
        s3.region_name = 'custom',
        s3.bucket_name = '{MINIO_BUCKET}',
        s3.credentials.access = '{MINIO_ACCESS}',
        s3.credentials.secret = '{MINIO_SECRET}',
        s3.endpoint_url = 'http://{MINIO_BUCKET}.{MINIO_ENDPOINT}',
        refresh.interval.sec = 1
    ) FORMAT PLAIN ENCODE PARQUET;
    """)

    count = 0
    for attempt in range(MAX_RETRIES):
        cur.execute("SELECT count(*) FROM mongo_e2e_verify")
        count = cur.fetchone()[0]
        if count >= NUM_DOCS:
            break
        print(f"[retry {attempt}] Got {count}/{NUM_DOCS} rows, waiting {RETRY_INTERVAL_SECS}s...")
        sleep(RETRY_INTERVAL_SECS)

    print(f"Verified {count} rows in parquet sink output (expected {NUM_DOCS})")
    assert count >= NUM_DOCS, f"Expected >= {NUM_DOCS} rows, got {count}"

    # Print all rows for inspection
    cur.execute("SELECT _id, payload FROM mongo_e2e_verify ORDER BY _id")
    rows = cur.fetchall()
    print(f"\n{'_id':<30} {'payload'}")
    print("-" * 80)
    for row in rows:
        print(f"{str(row[0]):<30} {row[1]}")


def cleanup_rw(cur):
    """Drop all RisingWave objects created by this test."""
    print("Cleaning up RisingWave objects...")
    for stmt in [
        "DROP TABLE IF EXISTS mongo_e2e_verify",
        "DROP SINK IF EXISTS mongo_e2e_parquet_sink",
        "DROP TABLE IF EXISTS mongo_e2e_source",
    ]:
        cur.execute(stmt)
    print("Cleanup complete.")


def cleanup_minio(minio_client):
    """Remove parquet test files from MinIO."""
    print("Cleaning up MinIO objects...")
    objects = minio_client.list_objects(MINIO_BUCKET, prefix=SINK_PATH, recursive=True)
    for obj in objects:
        minio_client.remove_object(MINIO_BUCKET, obj.object_name)
        print(f"  Deleted: {obj.object_name}")


def main():
    mongo_uri = get_mongo_uri()

    # Step 1: Seed MongoDB
    seed_mongodb(mongo_uri)

    # Step 2: Connect to RisingWave
    print(f"Connecting to RisingWave at {RW_HOST}:{RW_PORT}...")
    conn = psycopg2.connect(host=RW_HOST, port=RW_PORT, user="root", database="dev")
    conn.autocommit = True
    cur = conn.cursor()

    # Step 3: MinIO client
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=False,
    )

    try:
        # Step 4: Create RisingWave pipeline
        create_rw_pipeline(cur, mongo_uri)

        # Step 5: Wait for Parquet files in MinIO
        wait_for_parquet_files(minio_client)

        # Step 6: Verify row count by reading parquet back
        verify_row_count(cur)

        print("\nE2E test PASSED: mongo-oplog -> S3 Parquet sink produces valid parquet files")
    finally:
        # Always cleanup
        cleanup_rw(cur)
        cleanup_minio(minio_client)
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
