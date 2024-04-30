from io import BytesIO
from datetime import datetime
import argparse
import json
import pyarrow as pa
import pyarrow.parquet as pq

from .config import S3_BUCKET_NAME


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = f"Not a valid date: '{s}'. Expected format: 'YYYY-MM-DD'."
        raise argparse.ArgumentTypeError(msg)


def upload_json_to_s3(s3_client, logger, data, path):
    try:
        s3_client.put_object(Body=json.dumps(data), Bucket=S3_BUCKET_NAME, Key=path)
        logger.info(f"[✓] Data uploaded to S3 at {path}")
    except Exception as e:
        logger.error(f"[x] Error uploading data to S3: {str(e)}")


def write_df_parquet_to_s3(s3_client, logger, df, path):
    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df)

    # Write Parquet file to S3
    parquet_buffer = BytesIO()
    pq.write_table(table, parquet_buffer)

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME, Key=path, Body=parquet_buffer.getvalue()
    )

    logger.info("DataFrame successfully written to S3 in Parquet format")


def read_parquet_from_s3(path):
    # Read Parquet file from S3
    table = pq.read_table(path)

    # Convert to Pandas DataFrame
    df = table.to_pandas()

    return df
