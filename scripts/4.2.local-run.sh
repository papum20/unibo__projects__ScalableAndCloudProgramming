#!/bin/bash

if [[ ! -d scripts ]]; then
	echo "[ERROR] Scripts must be run from the project's root folder."
	exit 1
fi

source ./scripts/0.1.variables.sh


TIMESTAMP=$(date +'%Y%m%d-%H_%M_%S')
PATH_LOG4J_PROPERTIES="${PWD}/scripts/log4j.properties"
PATH_LOG="${PWD}/out/log${TIMESTAMP}.txt"

JVM_MEMORY=3G


# needed for logging (history-server)
mkdir /tmp/spark-events
start-master.sh
start-worker.sh \
	"spark://${HOSTNAME}:7077"
	#-c CORES
	# -m MEM
start-history-server.sh


echo "[RUN] Launching..."

echo "[Run] Launching. JVM memory is $JVM_MEMORY" > "$PATH_LOG"
spark-submit \
	--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$PATH_LOG4J_PROPERTIES" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$PATH_LOG4J_PROPERTIES" \
    --driver-memory $JVM_MEMORY \
	"$PATH_SRC_JAR" \
	2>&1 | tee -a "$PATH_LOG"
    #--master 'local[0]' \
