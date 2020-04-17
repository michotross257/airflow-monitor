# Airflow Monitor

Author: Michael Trossbach

Contact: mptrossbach@gmail.com

## Overview

The monitoring system checks the:
- runtime of all active and running DAGs
- on/off button status of all active DAGs

It compares the current value with an expected value and sends a slack alert if the runtime is greater than the computed threshold or if the current status of the on/off button is not equal to the expected one. The runtime threshold is recomputed each day by `condition_table_dag.py` based on the historical runtime data of a given DAG.

## Usage
1. Add the following Airflow connection (in the UI: `Admin` > `Connections` > `Create`):
  - `airflow_postgres`

    Specify the connection info for the Postgres DB associated with Airflow. I use the Puckel docker-airflow repo (see [here](https://github.com/puckel/docker-airflow)) to use Airflow and the default connection info as defined in the `docker-compose*.yml` files is:
    - `Conn Type`: Postgres
    - `Host`: postgres
    - `Schema`: airflow
    - `Login`: airflow
    - `Password`: airflow
    - `Port`: 5432

2. Add the following Airflow variables (in the UI: `Admin` > `Variables` > `Create`):
  - `slack_channel`

    The Slack channel or group to which messages will be sent. See [here](https://api.slack.com/methods/chat.postMessage#channels) for more details.

  - `slack_password`

    Slack API authentication token

3. Copy the two `*_dag.py` files to your dags folder or wherever Airflow looks for DAGs


4. Turn the two DAGs on (i.e. toggle the on/off button in the UI from off to on), then you're good to go.


### Update Expected Button Status of a DAG

Whereas runtimes are updated automatically, the expected button status of a DAG will only change if you manually change it. This is because the alternative is to update each time `condition_table_dag.py` runs with the current on/off button status and so the expected value would always be the same as the current value, which isn't desired (*you want to know if a DAG is switched off when it should be on and vice versa*).

> NOTE: This section presumes use of the puckel docker-airflow repo which uses Postgres as the backend DB

1. List Docker containers (your container IDs will be different)

```
$ docker ps
CONTAINER ID        IMAGE                          COMMAND                  CREATED             STATUS                    PORTS                                        NAMES
0966d8e60241        puckel/docker-airflow:1.10.9   "/entrypoint.sh webs…"   13 minutes ago      Up 13 minutes (healthy)   5555/tcp, 8793/tcp, 0.0.0.0:8080->8080/tcp   docker-airflow_webserver_1
fe1e9af0bb9f        postgres:9.6                   "docker-entrypoint.s…"   13 minutes ago      Up 13 minutes             5432/tcp                                     docker-airflow_postgres_1
```

2. Start an interactive bash shell on container using the container ID associated with `docker-airflow_postgres_1`

```
$ docker exec -it fe1e9af0bb9f sh
```

3. Start interactive shell to access Postgres

```
# psql -U airflow
```

4. Update expected button status of a given DAG

```
airflow=# update dag_condition set is_paused__expected = not is_paused__expected where dag_id = <dag_id>;
```

5. Exit Postgres shell

```
airflow-# \q
```

6. Exit Docker shell

```
# exit
```
