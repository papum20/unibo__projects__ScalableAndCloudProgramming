#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

source ./scripts/0.1.variables.sh

gcloud dataproc clusters start "$DATAPROC_CLUSTER_NAME" \
    --region="$DATAPROC_CLUSTER_REGION"

gcloud storage cp "$PATH_SRC_JAR" "gs://${DATAPROC_BUCKET_NAME}/${PATH_DST_JAR}"
#gcloud storage cp "$PATH_SRC_DATASET" "gs://${DATAPROC_BUCKET_NAME}/${PATH_DST_DATASET}"