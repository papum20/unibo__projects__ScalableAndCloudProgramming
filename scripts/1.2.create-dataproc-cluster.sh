#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

source ./scripts/0.1.variables.sh

gcloud dataproc clusters create "$DATAPROC_CLUSTER_NAME" \
    --region="$DATAPROC_CLUSTER_REGION" \
    --zone="$DATAPROC_CLUSTER_ZONE" \
	--bucket="$DATAPROC_BUCKET_NAME" \
	--temp-bucket="$DATAPROC_TMP_BUCKET_NAME" \
	--num-workers "$DATAPROC_CLUSTER_NUM_WORKERS" \
	--master-boot-disk-size "$DATAPROC_CLUSTER_MASTER_BOOT_DISK_SIZE" \
	--worker-boot-disk-size "$DATAPROC_CLUSTER_WORKER_BOOT_DISK_SIZE" \
	--master-machine-type="$DATAPROC_CLUSTER_MASTER_MACHINE_TYPE" \
	--worker-machine-type="$DATAPROC_CLUSTER_WORKER_MACHINE_TYPE"
	#--scopes storage-rw
	#--optional-components=JUPYTER \
	#--enable-component-gateway \

