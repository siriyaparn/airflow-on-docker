# Airflow on Docker

## Getting start
To running Airflow in Docker, Follow this instruction: [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
including create `dags`, `logs`, `plugins`, and `data` folders.

### Instruction
Setting the right Airflow user
When using Docker on Linux, you need to configure your host user ID and might set the group ID to 0. This ensures that the files created in dags, logs, and plugins directories are not owned by the root user. Follow these steps to configure them for Docker Compose:

```sh
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

You can also build the new container image with specified requirements
```sh
docker-compose build
```

First initialization of Airflow
```sh
docker-compose up airflow-init
```

Then, after initialization, start all containers:
```sh
docker-compose up -d
```

To stop containers
```sh
docker-compose down
```

Important: In the dag file in `dags/`, do not forget to apply database credentials and do not commit passwords or credential to git.

### Keeping credentials in Environment Variables
You can keep credentials in `.env` files. Then those variables will be set in container by `docker-compose.yml` in `environment:` section.

The example of `.env` file:
```sh
AIRFLOW_UID=
AIRFLOW_GID=
MYSQL_HOST=
MYSQL_PORT=
MYSQL_USER=
MYSQL_PASSWORD=
MYSQL_DB=
MYSQL_CHARSET=
```
