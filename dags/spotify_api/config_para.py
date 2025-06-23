from dotenv import load_dotenv
import os

load_dotenv()

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_secure = os.getenv("MINIO_SECURE", "False").lower() == "true"

mariadb_host = os.environ.get("MARIADB_HOST", "mariadb_spotify")
mariadb_port = os.environ.get("MARIADB_PORT")
mariadb_user = os.environ.get("MARIADB_DATA_USER")
mariadb_password = os.environ.get("MARIADB_DATA_PASSWORD") # password db
mariadb_database = os.environ.get("MARIADB_DATA_DATABASE")


#schema
MARIADB_TABLE_SCHEMA_ARTISTS = {
    'id': 'VARCHAR(255) PRIMARY KEY',  # ID của nghệ sĩ, dùng làm khóa chính
    'name': 'VARCHAR(255) NOT NULL',  # Tên nghệ sĩ, không được để trống
    'popularity': 'INT',              # Độ nổi tiếng (số nguyên)
    'genres': 'TEXT',                 # Danh sách genres, có thể dài nên dùng TEXT
    'followers_total': 'BIGINT',      # Tổng số người theo dõi, có thể là số lớn nên dùng BIGINT
    'external_url_spotify': 'VARCHAR(512)', # URL Spotify, đủ dài cho URL
    'image_url': 'VARCHAR(512)'       # URL ảnh, đủ dài cho URL
}

COLUMNS_ARTISTS = [
    'id', 'name', 'popularity', 'genres', 'followers_total', 'external_url_spotify', 'image_url'
]

