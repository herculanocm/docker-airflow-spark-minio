#!/bin/bash

# Get the current directory
CURRENT_DIR=$(pwd)

# Update the CURRENT_DIR variable in the .env file
if grep -q "CURRENT_DIR=" .env; then
    # If CURRENT_DIR exists in the .env file, update it
    sed -i "s|CURRENT_DIR=.*|CURRENT_DIR=$CURRENT_DIR|g" .env
else
    # If CURRENT_DIR does not exist in the .env file, add it
    echo "CURRENT_DIR=$CURRENT_DIR" >> .env
fi

# List of directories to check and create if they do not exist
directories=(
    "./airflow/dags"
    "./airflow/logs"
    "./airflow/plugins"
    "./airflow/config"
)

# Loop through each directory
for dir in "${directories[@]}"; do
    if [ ! -d "$dir" ]; then
        echo "Directory $dir does not exist. Creating..."
        mkdir -p "$dir"
        echo "Directory $dir created."
    else
        echo "Directory $dir already exists."
    fi
done

# Check if Docker network exists, if not create it
NETWORK_NAME="my-net"
if ! docker network ls | grep -q "$NETWORK_NAME"; then
    echo "Docker network $NETWORK_NAME does not exist. Creating..."
    docker network create --driver bridge "$NETWORK_NAME"
    echo "Docker network $NETWORK_NAME created."
else
    echo "Docker network $NETWORK_NAME already exists."
fi


docker build -t herculanocm/airflow:2.10.3 ./airflow/

docker build -t herculanocm/spark:3.4.1 ./spark/

docker compose up airflow-init

# Run Docker Compose
docker compose up