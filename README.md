# Data automation and processing engine for the Open Data Platform - (Take Off)

This is a simple straigtforward spark applcation and an Airflow control layer monorepo.

It is intended to be run locally, and is not designed to be run in a production environment.

## Getting Started

### Prerequisites

* [Docker](https://www.docker.com/)
* [Python 3.6](https://www.python.org/downloads/release/python-360/)
* [Airflow](https://airflow.apache.org/)
* [Spark](https://spark.apache.org/)
* [Scala](https://www.scala-lang.org/)
* [Hadoop](https://hadoop.apache.org/)
* [Hive](https://hive.apache.org/)
* [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)

### Installing and Running

* Clone the repo
* Install the requirements
* Run the bootstrap installation script for airflow
* Launch the airflow webserver
* Launch the airflow scheduler
* Schedule the spark jobs on Airflow

## Running the tests

* Run the tests locally:

```bash
python -m pytest
```

Test are run using pytest and are located in the `tests` directory.

Test coverage is provided by pytest-cov and can be run using:

```bash
python -m pytest --cov=.
```

* TODO: Add end to end tests, and integration tests, run the whole application
in ci and deploy to a staging environment.

## Deployment

* TODO: Add additional notes about how to deploy this on a live system

## Collaborators

* [**Mahdi Ben Ayed**](https://github.com/BenAyedMehdi)
* [**Louay Nadi**](https://github.com/louay321)

## Authors

* [**Marouane Skandaji**](https://github.com/marouenes)
