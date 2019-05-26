# docker-compose-service-sqlite

## Overview

This docker formation shows how to modify Senzing configuration using the
`stream-configuration.py service` command.

This docker formation brings up the following docker containers:

1. *[coleifer/sqlite-web](https://github.com/coleifer/sqlite-web)*
1. *[senzing/stream-configuration](https://github.com/Senzing/stream-configuration)*

### Contents

1. [Expectations](#expectations)
    1. [Space](#space)
    1. [Time](#time)
    1. [Background knowledge](#background-knowledge)
1. [Preparation](#preparation)
    1. [Prerequisite software](#prerequisite-software)
    1. [Clone repository](#clone-repository)
    1. [Create SENZING_DIR](#create-senzing_dir)
1. [Using docker-compose](#using-docker-compose)
    1. [Configuration](#configuration)
    1. [Run docker formation](#run-docker-formation)
    1. [View data](#view-data)
    1. [Test Senzing configuration API](#test-senzing-configuration-api)
1. [Cleanup](#cleanup)

## Expectations

### Space

This repository and demonstration require 7 GB free disk space.

### Time

Budget 2 hours to get the demonstration up-and-running, depending on CPU and network speeds.

### Background knowledge

This repository assumes a working knowledge of:

1. [Docker](https://github.com/Senzing/knowledge-base/blob/master/WHATIS/docker.md)
1. [Docker-compose](https://github.com/Senzing/knowledge-base/blob/master/WHATIS/docker-compose.md)

## Preparation

### Prerequisite software

The following software programs need to be installed:

1. [docker](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-docker.md)
1. [docker-compose](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/install-docker-compose.md)

### Clone repository

1. Set these environment variable values:

    ```console
    export GIT_ACCOUNT=senzing
    export GIT_REPOSITORY=stream-configuration
    ```

1. Follow steps in [clone-repository](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/clone-repository.md) to install the Git repository.

1. After the repository has been cloned, be sure the following are set:

    ```console
    export GIT_ACCOUNT_DIR=~/${GIT_ACCOUNT}.git
    export GIT_REPOSITORY_DIR="${GIT_ACCOUNT_DIR}/${GIT_REPOSITORY}"
    ```

### Create SENZING_DIR

If you do not already have an `/opt/senzing` directory on your local system, visit
[HOWTO - Create SENZING_DIR](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/create-senzing-dir.md).

## Using docker-compose

### Configuration

* **SENZING_DIR** -
  Path on the local system where
  [Senzing_API.tgz](https://s3.amazonaws.com/public-read-access/SenzingComDownloads/Senzing_API.tgz)
  has been extracted.
  See [Create SENZING_DIR](#create-senzing_dir).
  No default.
  Usually set to "/opt/senzing".

### Run docker formation

1. :pencil2: Set environment variables.  Example:

    ```console
    export SENZING_DIR=/opt/senzing
    ```

1. Launch docker-compose formation.  Example:

    ```console
    cd ${GIT_REPOSITORY_DIR}

    sudo \
      docker-compose --file docker-compose-service-sqlite.yaml up
    ```

### View data

1. SQLite is viewable at [localhost:8080](http://localhost:8080).
   The table modified by the following `curl` calls is `CFG_DSRC`.

### Test Senzing configuration API

1. :pencil2: Set environment variables.
   Example:

    ```console
    export SENZING_CONFIG_URL=http://localhost:5000
    export DSRC_ID=1999
    ```

1. Get existing Senzing data sources.
   Example:

    ```console
    curl -X GET ${SENZING_CONFIG_URL}/datasources
    ```

1. Add a new Senzing data source.
   Example:

    ```console
    curl -X POST \
      --header "Content-type: application/json" \
      --data '{"DSRC_ID":"'${DSRC_ID}'", "DSRC_CODE": "new-data-source"}' \
      ${SENZING_CONFIG_URL}/datasources
    ```

1. Get single data source.
   Example:

    ```console
    curl -X GET \
       ${SENZING_CONFIG_URL}/datasources/${DSRC_ID}
    ```

1. Update Senzing data source.
   Example:

    ```console
    curl -X PUT \
      --header "Content-type: application/json" \
      --data '{"DSRC_DESC": "A new description"}' \
       ${SENZING_CONFIG_URL}/datasources/${DSRC_ID}
    ```

1. Delete Senzing data source.
   Example:

    ```console
    curl -X DELETE \
       ${SENZING_CONFIG_URL}/datasources/${DSRC_ID}
    ```

## Cleanup

In a separate (or reusable) terminal window:

1. Use environment variable describe in "[Clone repository](#clone-repository)" and "[Configuration](#configuration)".
1. Run `docker-compose` command.

    ```console
    cd ${GIT_REPOSITORY_DIR}
    sudo docker-compose --file docker-compose-service-sqlite.yaml down
    ```

1. Delete SENZING_DIR.

    ```console
    sudo rm -rf ${SENZING_DIR}
    ```

1. Delete git repository.

    ```console
    sudo rm -rf ${GIT_REPOSITORY_DIR}
    ```
