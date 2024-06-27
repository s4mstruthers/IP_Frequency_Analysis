#!/bin/bash

# Define paths
CONTAINER_NAME="big-data"
REMOTE_PATH="/opt/hadoop/rubigdata/IP_Frequency_Data"
LOCAL_PATH="/Users/samstruthers/Documents/University/Year_2/Big_Data/Project"

# Find the main CSV part file in the remote path
PART_FILE=$(docker exec $CONTAINER_NAME sh -c "ls $REMOTE_PATH | grep '^part-.*.csv$' | head -n 1")

# Check if PART_FILE is not empty
if [ -n "$PART_FILE" ]; then
  # Full remote path to the part file
  FULL_REMOTE_FILE="$REMOTE_PATH/$PART_FILE"
  
  # Check if the file exists in the container
  if docker exec $CONTAINER_NAME sh -c "[ -f $FULL_REMOTE_FILE ]"; then
    # Copy the main CSV part file to the local machine
    docker cp "$CONTAINER_NAME:$FULL_REMOTE_FILE" "$LOCAL_PATH"

    echo "CSV file has been copied to $LOCAL_PATH"
  else
    echo "Could not find the file $FULL_REMOTE_FILE in the container."
  fi
else
  echo "No CSV part file found in $REMOTE_PATH"
fi