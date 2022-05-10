#!/bin/bash

CONTAINER_NAME=$(pwd | grep -o '[^/]*/[^/]*$' | tr "/" "_") # [parent_dir]_[current_dir]

CONTAINER_ID="$(docker ps -aqf "name=^$CONTAINER_NAME$")"

if [ -z "$CONTAINER_ID" ]
then
    echo "Creating new container $CONTAINER_NAME"
    docker run -it -d --platform linux/x86_64 --name $CONTAINER_NAME -v /$PWD:/app pyfony/pyre-check sleep infinity
else
    echo "Using existing container $CONTAINER_NAME"
fi

CONTAINER_ID="$(docker inspect --format='{{.Id}}' $CONTAINER_NAME)"

if [ "$( docker container inspect -f '{{.State.Status}}' $CONTAINER_ID )" == "exited" ]; then
    echo "Container $CONTAINER_ID is stopped, starting..."
    docker start $CONTAINER_ID
fi

docker exec -it $CONTAINER_ID //root/pyre_check.sh
