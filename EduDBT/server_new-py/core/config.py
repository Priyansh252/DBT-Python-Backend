import os

DB_NAME = os.getenv("DB_NAME", "DBTDatabase")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "root")
DB_PORT = int(os.getenv("DB_PORT", 5432))
