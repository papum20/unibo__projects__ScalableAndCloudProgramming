#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

if [[ $# -lt 1 ]] ; then
	echo "usage: $0 [-r] DIVIDE_FACTOR"
	echo "Split the dataset and save it in a new file (in its same directory)."
	echo "    DIVIDE_FACTOR: for a file of N rows, take N/DIVIDE_FACTOR rows"
	echo "    -r: take random rows (otherwise, take first rows)"
	exit 1
fi

source ./scripts/0.1.variables.sh


MODE=head

if [[ $1 == "-r" ]]; then
	MODE=random
	DIVIDE_FACTOR=$2
else
	DIVIDE_FACTOR=$1
fi

ORIGINAL_SIZE=$(wc -l < "$PATH_SRC_DATASET")
DIVIDED_SIZE=$(( ORIGINAL_SIZE/DIVIDE_FACTOR ))

_ORIGINAL_FILENAME=$(basename -- "$PATH_SRC_DATASET")
_ORIGINAL_EXTENSION="${_ORIGINAL_FILENAME##*.}"
_DIVIDED_FILENAME="${PATH_SRC_DATASET%.*}"


if [[ $MODE == "random" ]]; then
	DIVIDED_PATH="${_DIVIDED_FILENAME}_${DIVIDE_FACTOR}_random.csv"
	echo "Writing to path: $DIVIDED_PATH"
	# option 1: take n/factor random rows
	cat "$PATH_SRC_DATASET" | shuf | head -n $DIVIDED_SIZE  > $DIVIDED_PATH
elif [[ $MODE == "head" ]]; then
	DIVIDED_PATH="${_DIVIDED_FILENAME}_${DIVIDE_FACTOR}.csv"
	echo "Writing to path: $DIVIDED_PATH"
	# option 2: take first n/factor rows
	head -n $DIVIDED_SIZE "$PATH_SRC_DATASET" > $DIVIDED_PATH
fi