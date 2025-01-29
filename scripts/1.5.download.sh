#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

source ./scripts/0.1.variables.sh

mkdir -p "${PATH_LOCAL_OUT}"

gcloud storage cp "gs://${DATAPROC_BUCKET_NAME}/${PATH_REMOTE_OUT}/**" "${PATH_LOCAL_OUT}"

# merge in an unique csv file
find $PATH_LOCAL_OUT -type f -regex '.*part-[0-9]+[^\.crc]' -exec cat {} >> ${PATH_LOCAL_OUT_CSV} \;
