import io
import pandas as pd
from minio import Minio
from minio.error import S3Error
from spotify_api.config_para import minio_endpoint, minio_access_key, minio_secret_key, minio_secure

def upload_to_minio(data, bucket_name, object_name):
    try:
        #tạo client
        client = Minio(endpoint=minio_endpoint,
                       access_key=minio_access_key,
                       secret_key=minio_secret_key,
                       secure=minio_secure)
        #check bucket trước khi lưu data vào bucket
        if not client.bucket_exists(bucket_name=bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket {bucket_name} created.")
        else:
            print(f"Bucket {bucket_name} already exists.")

        #chuyển data đã kéo về từ API sang định dạng parquet trước khi upload lên minio
        df = pd.DataFrame(data)
        parquet_buffer= io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        #upload lên minio
        client.put_object(bucket_name,
                          object_name, 
                          data=parquet_buffer, 
                          length=parquet_buffer.getbuffer().nbytes,
                          content_type="application/x-parquet")
        print(f"Upload {object_name} to {bucket_name} sucessfully")
    except S3Error as e:
        print(f"Error uploading to MinIO: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during MinIO upload: {e}")
        raise
