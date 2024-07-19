# COVID-19 ETL Project Documentation

## Introduction

This ETL (Extract, Transform, Load) project aims to extract data on COVID-19 cases in the United States, transform it, and load it into a PostgreSQL database. The project is deployed using Apache Airflow for orchestration and AWS S3 for data buffering. The ultimate goal is to generate monthly reports on the number of cases and deaths, the daily average of new cases and deaths, and the days with the most new cases and deaths.

## 1. AWS Configuration

### 1.1. AWS Account Creation
- A new AWS account was created using the email `ecovis.test@gmail.com` for this project in order to implement the free tier.
- The user `data_admin` was created with an `AdministratorAccess` policy following AWS best practices.

### 1.2. AWS S3 Configuration
- An S3 bucket called `postgres-misc-ecovis` was created to be used as a data lake.
- The bucket access policies were configured to allow reading and writing from the administrator user and the necessary services. The following policy was added to the bucket:

```json
{
    "Version": "2012-10-17",
    "Id": "Policy1721236231332",
    "Statement": [
        {
            "Sid": "Stmt1721236229086",
            "Effect": "Allow",
            "Principal": {
                "AWS": "<your-aws-account-id>"
            },
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::postgres-misc-ecovis",
                "arn:aws:s3:::postgres-misc-ecovis/*"
            ]
        }
    ]
}
```

### 1.3. Configuración de PostgreSQL
- It was decided to use a PostgreSQL database implemented on the AWS RDS service using the free tier
- The database was configured to be accessible from Airflow, this implies making adjustments to the configured security groups and VPCs.
- The work environment within the database was established by creating the necessary users, roles and grants for a correct and secure workflow.

---
#### Configuration Queries

```sql
-- Create databases
CREATE DATABASE data_prod_ecovis;
CREATE DATABASE staging;

-- List users
SELECT usename FROM pg_shadow;
SELECT usename FROM pg_user;

-- Create roles and users
CREATE ROLE data_engineer WITH LOGIN PASSWORD '';
CREATE ROLE airflow_user WITH LOGIN PASSWORD '';
CREATE ROLE read_only_group;
CREATE ROLE write_group;

-- Create schemas
CREATE SCHEMA ecovis_test;
CREATE SCHEMA temp_tables;

-- Assign permissions to roles
GRANT CONNECT ON DATABASE data_prod_ecovis TO data_engineer;
GRANT CONNECT ON DATABASE data_prod_ecovis TO airflow_user;
GRANT USAGE ON SCHEMA ecovis_test TO data_engineer, airflow_user;
GRANT USAGE ON SCHEMA temp_tables TO data_engineer, airflow_user;
GRANT SELECT ON ALL TABLES IN SCHEMA ecovis_test TO read_only_group;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ecovis_test TO write_group;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA temp_tables TO write_group;

-- Add users to role groups
GRANT read_only_group TO data_engineer;
GRANT write_group TO airflow_user;

-- Create table
DROP TABLE IF EXISTS ecovis_test.covid_us;
CREATE TABLE ecovis_test.covid_us
(
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    cases INTEGER NOT NULL,
    deaths INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    deleted_at TIMESTAMP WITH TIME ZONE
);

```
![DB structure](https://github.com/user-attachments/assets/b3378ea6-72b0-4d32-928d-accedb8e64d5)

## 2. Apache Airflow Configuration

### 2.1. Instalación y Configuración de Airflow
- Apache Airflow was installed and configured in a Docker container.

To deploy Apache Airflow in the local environment, follow these steps:

#### Step 1: Build the Docker Image
- Navigate to the `docker/local` directory and build the Docker image. This step needs to be done only once:

```bash
cd docker/local
sudo docker compose build
```
After the first time, you only need to run 

```bash
cd docker/local
sudo docker compose up
```
#### Accessing the Airflow Web Interface
- After starting Airflow, you can access the Airflow web interface by navigating to http://localhost:8080 in your web browser.

### 2.2 Set up connections in Airflow UI
![image](https://github.com/user-attachments/assets/85611aaa-4f76-4648-8de3-69ad876cd219)

- The DB credentials created in the database configuration or the master user credentials obtained when the database is created must be entered.
- In the case of S3, it must be created with the secret keys provided by the AWS account administrator.

## 3. DAG development at Airflow
The code structure is as follows:

![image](https://github.com/user-attachments/assets/4dd60bcb-8348-4c37-919c-18df496222e5)

### 3.1. Hooks extension and standardization:
Some airflow Hooks were extended to handle a standard connection and usability of different class methods to interact with the database and storage in S3.

![image](https://github.com/user-attachments/assets/08b2e5df-8e63-44fe-83d7-ab22759c5ceb)


### 3.2. Data Transformation and Data Loading
Functions were developed to storage data in S3 and transform the data for persist it into PostgreSQL **(covid/main.py and covid/core.py)**

![image](https://github.com/user-attachments/assets/9a33a23f-0774-4771-9e78-a3fcf1edeeb7)


### 3.3. Creation of Views and Reports
SQL queries were developed to generate the required metrics **(reports/covid/reports.py)** as alternatives to the implementation of dbt models.

![image](https://github.com/user-attachments/assets/eb7538f3-6dc7-4719-a77e-fc1a8866e011)






