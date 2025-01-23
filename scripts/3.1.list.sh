#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

source ./scripts/0.1.variables.sh


echo -e "Buckets:"
gcloud storage ls

echo -e "\nDataproc clusters:"
gcloud dataproc clusters list --region="${DATAPROC_CLUSTER_REGION}"

echo -e "\nDataproc bucket files:"
gcloud storage ls "gs://${DATAPROC_BUCKET_NAME}"
