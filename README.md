# spike-stream-configuration

**WARNING**: This project is a "[spike](https://en.wikipedia.org/wiki/Spike_%28software_development%29)".

It is meant to test an idea.

It probably:

1. Is incomplete.
1. Doesn't work.
1. Is a work in-progress and may not be intended for final product status.

## Overview

The [stream-configuration.py](stream-configuration.py) python script is used to perform Senzing configuration via input from:

1. Local or URL-addressable file
1. RESTful micro-service
1. Kafka
1. RabbitMQ
1. STDIN

The `senzing/stream-configuration` docker image is a wrapper for use in docker formations (e.g. docker-compose, kubernetes).

To see all of the subcommands, run:

```console
$ ./stream-configuration.py --help
usage: stream-configuration.py [-h]
                               {url,service,kafka,rabbitmq,sleep,docker-acceptance-test}
                               ...

Configure Senzing metadata. For more information, see
https://github.com/senzing/stream-configuration

positional arguments:
  {url,service,kafka,rabbitmq,sleep,docker-acceptance-test}
                        Subcommands (SENZING_SUBCOMMAND):
    url                 Read JSON Lines from a URL addressable file.
    service             Receive HTTP requests.
    kafka               Read JSON Lines from Apache Kafka topic.
    rabbitmq            Read JSON Lines from RabbitMQ queue.
    sleep               Do nothing but sleep. For Docker testing.
    docker-acceptance-test
                        For Docker acceptance testing.

optional arguments:
  -h, --help            show this help message and exit
```

To see the options for a subcommand, run commands like:

```console
./stream-configuration.py kafka --help
```

## Demonstrate using Docker

### Create SENZING_DIR

1. If `/opt/senzing` directory is not on local system, visit
   [HOWTO - Create SENZING_DIR](https://github.com/Senzing/knowledge-base/blob/master/HOWTO/create-senzing-dir.md).

### Configuration

* **SENZING_DATABASE_URL** -
  Database URI in the form: `${DATABASE_PROTOCOL}://${DATABASE_USERNAME}:${DATABASE_PASSWORD}@${DATABASE_HOST}:${DATABASE_PORT}/${DATABASE_DATABASE}`
  Default:  [internal SQLite database]
* **SENZING_DEBUG** -
  Enable debug information. Values: 0=no debug; 1=debug.
  Default: 0.
* **SENZING_DIR** -
  Path on the local system where
  [Senzing_API.tgz](https://s3.amazonaws.com/public-read-access/SenzingComDownloads/Senzing_API.tgz)
  has been extracted.
  See [Create SENZING_DIR](#create-senzing_dir).
  No default.
  Usually set to "/opt/senzing".
* **SENZING_ENTRYPOINT_SLEEP** -
  Sleep, in seconds, before executing.
  0 for sleeping infinitely.
  [not-set] if no sleep.
  Useful for debugging docker containers.
  To stop sleeping, run "`unset SENZING_ENTRYPOINT_SLEEP`".
  Default: [not-set].
* **SENZING_HOST** -
  IP address for web micro-service.
  Default: 0.0.0.0
* **SENZING_INPUT_URL** -
  URL of source file.
  No default.
* **SENZING_KAFKA_BOOTSTRAP_SERVER** -
  Hostname and port of Kafka server.
  Default: "localhost:9092"
* **SENZING_KAFKA_GROUP** -
  Kafka group.
  Default: "senzing-kafka-group"
* **SENZING_KAFKA_TOPIC** -
  Kafka topic.
  Default: "senzing-kafka-topic"
* **SENZING_LOG_LEVEL** -
  Level of logging. {notset, debug, info, warning, error, critical}.
  Default: info
* **SENZING_PORT** -
  IP address for web micro-service.
  Default: 5000
* **SENZING_RABBITMQ_HOST** -
  Host name of the RabbitMQ exchange.
  Default: "localhost:5672"
* **SENZING_RABBITMQ_PASSWORD** -
  The password for the RabbitMQ queue.
  Default: "bitnami"
* **SENZING_RABBITMQ_QUEUE** -
  Name of the RabbitMQ queue used for communication.
  Default: "senzing-rabbitmq-queue"
* **SENZING_RABBITMQ_USERNAME** -
  The username for the RabbitMQ queue.
  Default: "user"
* **SENZING_SLEEP_TIME** -
  Amount of time to sleep, in seconds for `stream-loader.py sleep` subcommand.
  Default: 600.
* **SENZING_SUBCOMMAND** -
  Identify the subcommand to be run. See `stream-loader.py --help` for complete list.

1. To determine which configuration parameters are use for each `<subcommand>`, run:

    ```console
    ./stream-configuration.py <subcommand> --help
    ```
