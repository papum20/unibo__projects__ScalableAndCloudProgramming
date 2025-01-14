#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

source ./scripts/0.1.variables.sh


PATH_LOG4J_PROPERTIES="${PWD}/scripts/log4j.properties"

JVM_MEMORY=3G


start-master.sh

start-worker.sh \
	"spark://${HOSTNAME}:7077"
	#-c CORES
	# -m MEM


echo "[RUN] Launching..."

spark-submit \
	--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$PATH_LOG4J_PROPERTIES" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$PATH_LOG4J_PROPERTIES" \
    --driver-memory $JVM_MEMORY \
	"$PATH_SRC_JAR"
    #--master 'local[0]' \
