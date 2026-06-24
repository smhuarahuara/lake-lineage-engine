-- Crea usuario y base de datos para Airflow
-- La base "metastore" ya existe por POSTGRES_DB en el .env

CREATE USER airflow WITH PASSWORD 'airflow123';
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;