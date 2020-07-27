ARG BASE_IMAGE=senzing/senzing-base:1.5.2
FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2019-07-23

LABEL Name="senzing/stream-configuration" \
      Maintainer="support@senzing.com" \
      Version="1.0.0"

HEALTHCHECK CMD ["/app/healthcheck.sh"]

# Run as "root" for system installation.

USER root

# Install packages via apt.

RUN apt-get update \
 && apt-get -y install \
    librdkafka-dev \
 && rm -rf /var/lib/apt/lists/*

# Install packages via PIP.

RUN pip3 install \
    configparser \
    confluent-kafka \
    Flask \
    flask_api \
    psutil \
    pika \
    six

# Copy files from repository.

COPY ./rootfs /
COPY ./stream-configuration.py /app/

# Make non-root container.

USER 1001

# Runtime execution.

ENV SENZING_DOCKER_LAUNCHED=true

WORKDIR /app
ENTRYPOINT ["/app/stream-configuration.py" ]
