#!/usr/bin/python3
# -*- coding:utf-8 -*-
from common import *

cmd = """%s/bin/java
 -Xmx%dm
 -Dsocialite.master=%s
 -Dsocialite.worker.num=%d
 -Dlog4j.configuration=file:%s
 -Dsocialite.worker.num_threads=4
 -Dsocialite.output.dir=%s
 -classpath %s
 socialite.dist.worker.WorkerNode 2>&1 | tee %s""" % (
    JAVA_HOME,
    HEAP_SIZE, MASTER_HOSTNAME, WORKER_NUM, SOCIALITE_PREFIX + '/conf/log4j.properties',
    SOCIALITE_PREFIX + '/gen', class_path, SOCIALITE_PREFIX + "/logs/master.log")
cmd = cmd.replace('\n', '')
for worker_hostname in WORKER_HOSTNAME_LIST:
    os.system("ssh -n %s \"pkill -f WorkerNode\"" % worker_hostname)
    os.system("ssh -f -n %s \"%s\"" % (worker_hostname, cmd))
