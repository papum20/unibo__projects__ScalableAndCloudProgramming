#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

sbt compile \
	package