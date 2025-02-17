#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi


ID=1733223004
PREFIX="scalable24"
TIMESTAMP="$(date +'%Y%m%d-%H_%M_%S')"

# main bucket for dataproc
export DATAPROC_BUCKET_CLASS=regional
export DATAPROC_BUCKET_NAME=${PREFIX}-dataproc-staging-bucket-${ID}
export DATAPROC_BUCKET_ZONE=us-west1

# bucket for dataproc temporary files
export DATAPROC_TMP_BUCKET_CLASS=regional
export DATAPROC_TMP_BUCKET_NAME=${PREFIX}-dataproc-temp-bucket-${ID}
export DATAPROC_TMP_BUCKET_ZONE=us-west1


# dataproc cluster
export DATAPROC_CLUSTER_NAME=${PREFIX}-dataproc-cluster-${ID}
#export DATAPROC_CLUSTER_REGION=europe-west8
export DATAPROC_CLUSTER_REGION=us-west1
# auto-zone
export DATAPROC_CLUSTER_ZONE=""
#export DATAPROC_CLUSTER_ZONE=us-west1-b
export DATAPROC_CLUSTER_NUM_WORKERS=3
#3
export DATAPROC_CLUSTER_MASTER_BOOT_DISK_SIZE=240
export DATAPROC_CLUSTER_WORKER_BOOT_DISK_SIZE=240
#export DATAPROC_CLUSTER_MASTER_MACHINE_TYPE=n2-standard-2
#export DATAPROC_CLUSTER_WORKER_MACHINE_TYPE=n2-standard-2
export DATAPROC_CLUSTER_MASTER_MACHINE_TYPE=n2-highmem-2
export DATAPROC_CLUSTER_WORKER_MACHINE_TYPE=n2-highmem-2
#DATAPROC_SCALEUP_TO_NUMWORKERS=50
#DATAPROC_SCALEDOWN_TO_NUMWORKERS=2


# paths
export PATH_SRC_JAR=${PWD}/target/scala-2.12/scalable_2.12-0.1.0.jar
export PATH_SRC_DATASET=${PWD}/src/main/resources/order_products.csv
#export PATH_SRC_DATASET=${PWD}/src/main/resources/order_products_head100.csv
export PATH_DST_JAR=scalable.jar
export PATH_DST_DATASET=order_products.csv

export PATH_REMOTE_OUT=out
export PATH_LOCAL_OUT=${PWD}/out/${TIMESTAMP}
export PATH_LOCAL_OUT_CSV=${PWD}/out/${TIMESTAMP}.csv