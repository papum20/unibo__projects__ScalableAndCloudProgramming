#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

source ./0.1.variables.sh

gcloud dataproc clusters delete -q "$DATAPROC_CLUSTER_NAME" \
	--region "$DATAPROC_CLUSTER_REGION"
