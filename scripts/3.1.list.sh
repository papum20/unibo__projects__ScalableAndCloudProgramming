#!/bin/bash

source ./0.1.variables.sh

echo -e "Buckets:"
gcloud storage ls

echo -e "\nDataproc clusters:"
gcloud dataproc clusters list --region="${DATAPROC_CLUSTER_REGION}"

echo -e "\nDataproc bucket files:"
gcloud storage ls gs://${DATAPROC_BUCKET_NAME}
