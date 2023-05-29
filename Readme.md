# Introduction
This project is an Airflow ELT demo built on AWS, using Docker-compose as the base component to construct a Data Warehouse with S3 and Redshift.

# DAGs
### create_table_dag
This is used to initialize the table and will only be executed once.
### aws_etl_dag
This is the main DAG, write S3 data to Redshift for subsequent ELT processing.

# Plugins
### operators
To increase the readability and extensibility of the code, repeat operations are managed by operators. The following is an explanation of ELT in the order of execution.

* **stage_redshift.py**
First extract the data from S3 to Redshift for temporary storage.

* **load_fact.py**
Next, import fact data from staging table.

* **load_dimension.py**
Next, import dimension data from staging table.

* **data_quality.py**
Finally, to verify the correctness of the data, e.g., whether a value appears in a specific field that should not appear.

### operators.helpers
This is a subfolder of operators, used to store sql syntax, separating sql syntax helps readability and maintainability.
* **create_tables.py**
  This bind with DAG once_create_table to initialize tables.
* **sql_queries.py**
  This bind with ELT operators.

# Config
### Variables
Please import **config/variables.json** file into airflow to run DAG properly, see screenshots/variable_screenshot.png for more info.
* **my-redshift-service-role**
  This is a role with access rights to S3 and Redshift, will be used when performing ELT.
* **s3_json_path**
  This is the path for columns mapping, which is used when reading S3 data.
* **s3_log_data** & **s3_song_data**
  These are the paths to the dataset from S3.

### Connections
Before you run the DAGs, you should create airflow connections below:
_(This is demo only and no longer valid.)_
* **aws_credentials**
    This is the AWS access key from AWS IAM Management. (config/awsuser_accessKeys.csv)
* **redshift**
  This needs the correct connection id **redshift**, please see screenshots/redshift_conn_screenshot.png for more info.

# Docker-compose
### Settings
* **docker-compose.yaml**
```
version: '3'
x-airflow-common:
  &airflow-common
  # In order to add custom dependencies or upgrade provider packages you can use your extended image.
  # Comment the image line, place your Dockerfile in the directory where you placed the docker-compose.yaml
  # and uncomment the "build" line below, Then run `docker-compose build` to build the images.
  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.4.2}
  # build: .
  environment:
  ...
```
* **.env**
```
AIRFLOW_IMAGE_NAME=apache/airflow:2.4.2
AIRFLOW_UID=50000
```

### Run it
When you are sure you have **docker-compose.yaml** and **.env** under the root path, run below:
```
cd aws-ariflow-elt
docker-compose up -d
```
And then make sure the service is activated correctly.
```
docker ps
```
docker ps will be:
![image](https://github.com/hugohu0224/aws-ariflow-elt/blob/main/screenshots/docker_ps.png)
# Others
DAG successfully run:
![image](https://github.com/hugohu0224/aws-ariflow-elt/blob/main/screenshots/dag_finished.png)
