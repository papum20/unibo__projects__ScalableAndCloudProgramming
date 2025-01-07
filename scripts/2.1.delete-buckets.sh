#!/bin/bash

source ./0.1.variables.sh

# rm buckets and data
gcloud storage rm -r \
	gs://${DATAPROC_BUCKET_NAME} \
	gs://${DATAPROC_TMP_BUCKET_NAME}
