#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

source ./scripts/0.1.variables.sh

gcloud dataproc jobs submit spark --cluster="$DATAPROC_CLUSTER_NAME" \
	--bucket="${DATAPROC_BUCKET_NAME}" \
    --region="${DATAPROC_CLUSTER_REGION}" \
    --driver-log-levels org.apache.spark=INFO,com.google.cloud.hadoop.gcsio=INFO \
    --properties=spark.executor.memory=4g,spark.executor.cores=2,spark.driver.memory=4g,spark.sql.shuffle.partitions=50 \
    --jar="gs://${DATAPROC_BUCKET_NAME}/${PATH_DST_JAR}" \
    -- "gs://${DATAPROC_BUCKET_NAME}/${PATH_DST_DATASET}" "gs://${DATAPROC_BUCKET_NAME}/out_main3"
    #-- false "${DATAPROC_BUCKET_NAME}"
