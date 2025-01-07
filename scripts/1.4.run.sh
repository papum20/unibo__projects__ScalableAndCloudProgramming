#!/bin/bash

source ./0.1.variables.sh

gcloud dataproc jobs submit spark --cluster=$DATAPROC_CLUSTER_NAME \
	--bucket=${DATAPROC_BUCKET_NAME} \
    --region=${DATAPROC_CLUSTER_REGION} \
    --jar="gs://${DATAPROC_BUCKET_NAME}/${PATH_DST_JAR}"