#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

source ./scripts/0.1.variables.sh

gcloud storage buckets create "gs://${DATAPROC_BUCKET_NAME}" \
	-c "$DATAPROC_BUCKET_CLASS" \
	-l "$DATAPROC_BUCKET_ZONE" \
	`# without this, there are problems with ACLs, unless specific permissions for the default user are enabled.` \
	--uniform-bucket-level-access

gcloud storage buckets create "gs://${DATAPROC_TMP_BUCKET_NAME}" \
	-c "$DATAPROC_TMP_BUCKET_CLASS" \
	-l "$DATAPROC_TMP_BUCKET_ZONE" \
	--uniform-bucket-level-access