#!/usr/bin/env bash
BIN=`dirname "$0"`
BIN=`cd "$BIN"; pwd`

. ${BIN}/common.sh
CODE_CLASSPATH=${SOCIALITE_PREFIX}/out/production/socialite
java -Xmx2G -Dsocialite.worker.num=2 -Dsocialite.master=master -Dlog4j.configuration=file:${SOCIALITE_PREFIX}/conf/log4j.properties -cp ${CODE_CLASSPATH}:${JAR_PATH} socialite.dist.worker.WorkerNode