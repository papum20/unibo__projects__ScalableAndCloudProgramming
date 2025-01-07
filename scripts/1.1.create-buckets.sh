#!/bin/bash

source ./0.1.variables.sh

gcloud storage buckets create gs://${DATAPROC_BUCKET_NAME} \
	-c $DATAPROC_BUCKET_CLASS \
	-l $DATAPROC_BUCKET_ZONE \
	--uniform-bucket-level-access

gcloud storage buckets create gs://${DATAPROC_TMP_BUCKET_NAME} \
	-c $DATAPROC_TMP_BUCKET_CLASS \
	-l $DATAPROC_TMP_BUCKET_ZONE \
	--uniform-bucket-level-access