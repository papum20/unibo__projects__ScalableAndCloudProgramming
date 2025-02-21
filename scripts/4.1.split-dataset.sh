#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

if [[ $# -lt 1 ]] ; then
	echo "usage: $0 DIVIDE_FACTOR"
	exit 1
fi

source ./scripts/0.1.variables.sh


DIVIDE_FACTOR=$1
ORIGINAL_SIZE=$(wc -l < "$PATH_SRC_DATASET")
DIVIDED_SIZE=$(( ORIGINAL_SIZE/DIVIDE_FACTOR ))

_ORIGINAL_FILENAME=$(basename -- "$PATH_SRC_DATASET")
_ORIGINAL_EXTENSION="${_ORIGINAL_FILENAME##*.}"
_DIVIDED_FILENAME="${PATH_SRC_DATASET%.*}"
DIVIDED_PATH="${_DIVIDED_FILENAME}_${DIVIDE_FACTOR}.csv"

echo "Writing to path: $DIVIDED_PATH"

head -n $DIVIDED_SIZE "$PATH_SRC_DATASET" > $DIVIDED_PATH
