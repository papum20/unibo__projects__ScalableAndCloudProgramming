#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

source ./0.1.variables.sh

# rm buckets and data
gcloud storage rm -r \
	"gs://${DATAPROC_BUCKET_NAME}" \
	"gs://${DATAPROC_TMP_BUCKET_NAME}"
