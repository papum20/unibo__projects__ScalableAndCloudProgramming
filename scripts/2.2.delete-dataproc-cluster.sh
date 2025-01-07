#!/bin/bash

source ./0.1.variables.sh

gcloud dataproc clusters delete -q $DATAPROC_CLUSTER_NAME \
	--region $DATAPROC_CLUSTER_REGION
