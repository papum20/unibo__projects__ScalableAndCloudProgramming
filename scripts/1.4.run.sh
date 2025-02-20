#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

source ./scripts/0.1.variables.sh

gcloud dataproc clusters start "$DATAPROC_CLUSTER_NAME" \
    --region="$DATAPROC_CLUSTER_REGION"

# remove recursively
gcloud storage rm "gs://${DATAPROC_BUCKET_NAME}/${PATH_REMOTE_OUT}/**"

if [[ $DATAPROC_CLUSTER_NUM_WORKERS == 1 ]] ; then
	# with 1 worker, need more memory
	gcloud dataproc jobs submit spark --cluster="$DATAPROC_CLUSTER_NAME" \
		--bucket="${DATAPROC_BUCKET_NAME}" \
		--region="${DATAPROC_CLUSTER_REGION}" \
		 --properties=spark.executor.memory=8g \
		--jar="gs://${DATAPROC_BUCKET_NAME}/${PATH_DST_JAR}" \
		-- 10 false "${DATAPROC_BUCKET_NAME}" "${PATH_REMOTE_OUT}"
		#--driver-log-levels org.apache.spark=INFO,com.google.cloud.hadoop.gcsio=INFO \
		#--properties=spark.executor.memory=4g,spark.executor.cores=2,spark.driver.memory=4g,spark.sql.shuffle.partitions=50 \
		#-- "gs://${DATAPROC_BUCKET_NAME}/${PATH_DST_DATASET}" "gs://${DATAPROC_BUCKET_NAME}/${PATH_REMOTE_OUT}"
else
	gcloud dataproc jobs submit spark --cluster="$DATAPROC_CLUSTER_NAME" \
		--bucket="${DATAPROC_BUCKET_NAME}" \
		--region="${DATAPROC_CLUSTER_REGION}" \
		--jar="gs://${DATAPROC_BUCKET_NAME}/${PATH_DST_JAR}" \
		-- 10 false "${DATAPROC_BUCKET_NAME}" "${PATH_REMOTE_OUT}"
fi