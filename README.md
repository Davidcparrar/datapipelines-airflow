# Datapipelines-Airflow
This is a repo that contains the DAGs to process the Sparkify data to a Redshift database

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