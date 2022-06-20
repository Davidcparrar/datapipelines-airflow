# Datapipelines-Airflow
This is a repo that contains the DAGs to process the Sparkify data to a Redshift database

## Project structure

The project has been created with the following structure:

```bash
├── LICENSE
├── README.md
├── create_tables.sql
├── dags
│   ├── load_dimension_subdag.py
│   └── sparkify_etl.py
├── docker-compose.yml
├── docker-compose2.yml
├── dwh_template.cfg
├── iac_redshift.py
├── plugins
│   ├── __init__.py
│   ├── helpers
│   │   ├── __init__.py
│   │   └── sql_queries.py
│   └── operators
│       ├── __init__.py
│       ├── data_quality.py
│       ├── load_dimension.py
│       ├── load_fact.py
│       └── stage_redshift.py
└── requirements.txt
```

- dwh_template.cfg: Template for the configuration file. Fill in the missing information and rename the file to dwh.cfg
- docker-compose.yml: Docker compose file for Airflow 1.10.15.
- docker-compose2.yml: Docker compose file for Airflow 2.2.3.
- iac_redshift.py: Python utility that creates and deletes an AWS Redshift Cluster (IaC).
- requirements.txt: requirements for python env.
- create_tables.sql: Sql queries to create the tables inside the Redshift Cluster
- dags/load_dimension_subdag.py: Contains the subDAG that loads dimensions and checks quality
- dags/sparkify_etl.py: Contains the main DAG that loads the Sparkify data to REedshift
- plugins/helpers/sql_queries.py: Contains the SQL queries to get the dimensions data
- plugins/operators/*.py: Contains the 4 operators used int the DAG

## Usage

Before running the Airtflow DAG make sure to create the Redshift cluster and create the tables with the SQL commands provided in the create_tables.sql script.

After that, copy the files into an Airflow deployment and run the DAG from the Airflow UI. 

### Cluster administration

Create the cluster

```bash
python iac_redshift.py --create
```

After a while check if the cluster is available by running the status command

```bash
python iac_redshift.py --status
```

This will return a table and if the status is `available` it will also return the cluster address and the IAM role. Please fill in these values into the configuration file. CLUSTER-HOST and IAM_ROLE-NAME respectively.

The cluster can be deleted running to avoid innecessary costs while developing:

```bash
python iac_redshift.py --delete
```

> :warning: **This will delete the cluster and all the information with it. After this point all data will be lost and both the creation and uploading scripts will be needed to recreate the cluster.** 

### Table creation

The file create_tables.sql contains the SQL instructions to create the tables. Run the 

- stagingEvents : Staging table that maps the S3 Log data into the cluster.
- stagingSongs: Staging table that maps the S3 Song data into the cluster.

And the Schema for OLAP.

- songplays
- users
- songs
- artists
- time 

## Airflow with Docker

This process requires Airflow to run. For local development and testing the reader can ry and follow [this](https://towardsdatascience.com/setting-up-apache-airflow-with-docker-compose-in-5-minutes-56a1110f4122) tutorial to set up Airflow with Docker or check [this](https://github.com/apache/airflow/blob/main/docs/apache-airflow/start/docker-compose.yaml) from Apache Airflow itself. For Airflow version 1.10.15 check this [repo](https://github.com/xnuinside/airflow_in_docker_compose)

To run the container

```bash
docker-compose up
```

### Trouble shooting

Sometimes if the volumes are not deleted before the container is run this error may occur

```
service "airflow-init" didn't completed successfully: exit 1
```

Run and try again

```bash
docker-compose down --volumes --rmi all
```

Permissions:

VS Code: NoPermissions (FileSystemError): Error: EACCES: permission denied

[Solved](https://stackoverflow.com/questions/66496890/vs-code-nopermissions-filesystemerror-error-eacces-permission-denied)

```
sudo chown -R username path 
```

Reviewing loading errors in Redshift

```sql
SELECT filename, starttime, colname, type, raw_line, col_length, err_code, err_reason FROM stl_load_errors ORDER BY starttime DESC;
```