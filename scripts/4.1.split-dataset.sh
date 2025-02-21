#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

if [[ $# -lt 2 ]] ; then
	echo "usage: $0 DIVIDE_FACTOR"
fi

source ./scripts/0.1.variables.sh


DIVIDE_FACTOR=$1
ORIGINAL_SIZE=$(wc -l "$PATH_SRC_DATASET")
DIVIDED_SIZE=$(( $ORIGINAL_SIZE/$DIVIDE_FACTOR ))
DIVIDED_PATH="$PATH_SRC_DATASET"


head -n $DIVIDED_SIZE "$PATH_SRC_DATASET" >
