import pymysql
import io
import pandas as pd
from minio import Minio
from minio.error import S3Error


from spotify_api.config_para import (mariadb_host, mariadb_port, mariadb_user,
    mariadb_password, mariadb_database,
    MARIADB_TABLE_SCHEMA_ARTISTS, COLUMNS_ARTISTS,
    minio_endpoint, minio_access_key, minio_secret_key, minio_secure)

def create_table(table_name, schema):
    try:
        with pymysql.connect(host=mariadb_host,
                             port=int(mariadb_port),
                             user=mariadb_user,
                             database=mariadb_database,
                             password=mariadb_password
                            )as conn:
                                with conn.cursor() as cursor:
                                    create_table_query= f"""
                                    CREATE TABLE IF NOT EXISTS `{table_name}`(
                                    {", ".join([f"`{col}` {dtype}" for col, dtype in schema.items()])}
                                    );
                                    """
                                    cursor.execute(create_table_query)
                                    conn.commit()
                                    print(f"Table {table_name} created")

    except pymysql.Error as e:
        print(f"Error when create table '{table_name}': {e}")
        raise
    except Exception as e:
        print(f"Unexpected error when create table in MariaDB: {e}")
        raise



def importFile_from_minio_to_mariadb(bucket_name, object_name, mariadb_table, columns_order):
    client = Minio(
            endpoint=minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=minio_secure
            )
    try:
        response =client.get_object(bucket_name, object_name)
        data_bytes= response.read()
        response.close()
        response.release_conn()
        print(f"Downloaded {object_name}")

        parquet_buffer = io.BytesIO(data_bytes)
        df = pd.read_parquet(parquet_buffer, engine='pyarrow') # Vẫn cần 'pyarrow' để đọc Parquet
        print(f"Successfully read DataFrame from memory buffer. Rows: {len(df)}")

        if df.empty:
            print("DataFrame is empty, no data to import.")
            return
        # Đảm bảo thứ tự cột trước khi chèn vào DB
        df = df[columns_order]
        #Tạo table nếu chưa tồn tại
        create_table('artists_vpop', MARIADB_TABLE_SCHEMA_ARTISTS)

        with pymysql.connect(host=mariadb_host,
                             port=int(mariadb_port),
                             user=mariadb_user,
                             database=mariadb_database,
                             password=mariadb_password
                            )as conn:
            with conn.cursor() as cursor:
                #truncate dữ liệu cũ
                print(f"Truncating table '{mariadb_table}' to clear old data...")
                cursor.execute(f"TRUNCATE TABLE `{mariadb_table}`;")
                conn.commit()
                print(f"Table '{mariadb_table}' truncated successfully.")

                #insert dữ liệu mới
                cols = ", ".join([f"`{col}`" for col in columns_order])
                placeholders = ", ".join(["%s"] * len(columns_order))
                insert_sql = f"INSERT INTO `{mariadb_table}` ({cols}) VALUES ({placeholders})"
                
                data_to_insert = [tuple(row) for row in df.values]

                print(f"Inserting {len(data_to_insert)} rows into MariaDB...")
                try:
                    cursor.executemany(insert_sql, data_to_insert)
                    conn.commit()
                    print(f"Successfully inserted {len(data_to_insert)} rows into '{mariadb_table}'.")

                except pymysql.err.IntegrityError as ie:
                    print(f"Batch insert failed due to integrity error (e.g., duplicate key): {ie}. Rolling back.")
                    conn.rollback()
                except Exception as e:
                    print(f"Error during batch insert into MariaDB: {e}. Rolling back.")
                    conn.rollback()
                    raise

    except S3Error as e:
        print(f"MinIO error during import: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during import from MinIO to MariaDB: {e}")
        raise

