#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

source ./scripts/0.1.variables.sh


PATH_LOG4J_PROPERTIES="${PWD}/scripts/log4j.properties"


start-master.sh

start-worker.sh \
	"spark://${HOSTNAME}:7077"
	#-c CORES
	# -m MEM


spark-submit \
	--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$PATH_LOG4J_PROPERTIES" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$PATH_LOG4J_PROPERTIES" \
	`#--master local[0]` \
	"$PATH_SRC_JAR"
