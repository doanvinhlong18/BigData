-- Chạy tự động 1 lần khi postgres container khởi động lần đầu.
-- POSTGRES_DB=bigdata được Docker image tự tạo.

CREATE DATABASE mlflow
    WITH OWNER = admin ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8' LC_CTYPE = 'en_US.utf8'
    TEMPLATE = template0;

CREATE DATABASE airflow
    WITH OWNER = admin ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8' LC_CTYPE = 'en_US.utf8'
    TEMPLATE = template0;

GRANT ALL PRIVILEGES ON DATABASE mlflow  TO admin;
GRANT ALL PRIVILEGES ON DATABASE airflow TO admin;
GRANT ALL PRIVILEGES ON DATABASE bigdata TO admin;