import os
from contextlib import contextmanager
from datetime import datetime
from typing import Union
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import IOManager, OutputContext, InputContext
from minio import Minio

@contextmanager
def connect_minio(config):
    """Kết nối MinIO bằng thông tin cấu hình."""
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False,
    )
    try:
        yield client
    except Exception as e:
        raise Exception(f"MinIO connection error: {e}")

class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        """Tạo đường dẫn lưu file trong MinIO."""
        layer, schema, table = context.asset_key.path
        
        if context.has_asset_partitions:
            partition_key = context.asset_partition_key
            key = f"{layer}/{schema}/{partition_key}/{table.replace(f'{layer}_', '')}.pq"
        else:
            key = f"{layer}/{schema}/{table.replace(f'{layer}_', '')}.pq"
        
        tmp_file_path = f"/tmp/file-{datetime.today().strftime('%Y%m%d%H%M%S')}-{table}.parquet"
        return key, tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        """Lưu DataFrame dưới dạng Parquet và upload lên MinIO."""
        key_name, tmp_file_path = self._get_path(context)
        bucket_name = self._config.get("bucket")
        
        try:
            # Ghi DataFrame ra file Parquet
            table = pa.Table.from_pandas(obj)
            pq.write_table(table, tmp_file_path)
            
            # Kết nối MinIO và kiểm tra bucket
            with connect_minio(self._config) as client:
                if not client.bucket_exists(bucket_name):
                    client.make_bucket(bucket_name)
                
                client.fput_object(bucket_name, key_name, tmp_file_path)
            
            row_count = len(obj)
            context.add_output_metadata({"path": key_name, "records": row_count})
            
            # Xóa file tạm nếu tồn tại
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
            context.log.info(f"Uploaded {key_name} to {bucket_name} in MinIO.")
        except Exception as e:
            context.log.error(f"Error uploading to MinIO: {e}")
            raise

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Tải file Parquet từ MinIO về và đọc vào DataFrame."""
        bucket_name = self._config.get("bucket")
        key_name, tmp_file_path = self._get_path(context)
        
        try:
            # Kết nối MinIO và tải file
            with connect_minio(self._config) as client:
                if not client.bucket_exists(bucket_name):
                    client.make_bucket(bucket_name)
                
                client.fget_object(bucket_name, key_name, tmp_file_path)
            
            # Đọc file Parquet
            df = pd.read_parquet(tmp_file_path)
            
            # Xóa file tạm nếu tồn tại
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
            
            return df
        except Exception as e:
            context.log.error(f"Error downloading from MinIO: {e}")
            raise
