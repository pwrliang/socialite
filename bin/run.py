#!/usr/bin/python3
# -*- coding:utf-8 -*-
from common import *

if len(sys.argv) != 2:
    raise IOError("Datalog Program is nedded")

PROG_PATH = sys.argv[1]
if not os.path.exists(PROG_PATH):
    raise IOError("Invalid program path %s" % PROG_PATH)

cmd = """%s/bin/mpirun
 --prefix %s
 --machinefile %s
 %s/bin/java
 -Xmx%dm
 -Dsocialite.master=%s
 -Dsocialite.worker.num=%d
 -Dlog4j.configuration=file:%s
 -Dsocialite.worker.num_threads=4
 -Dsocialite.output.dir=%s
 -classpath %s
 socialite.async.DistEntry %s 2>&1 | tee %s""" % (
    MPI_HOME, MPI_HOME, MACHINE_FILE, JAVA_HOME,
    HEAP_SIZE, MASTER_HOSTNAME, WORKER_NUM, SOCIALITE_PREFIX + '/conf/log4j.properties',
    SOCIALITE_PREFIX + '/gen', class_path, PROG_PATH, SOCIALITE_PREFIX + "/logs/master.log")
cmd = cmd.replace('\n', '')
print(cmd)
os.system('pkill -f DistEntry')
os.system(cmd)
os.system('pkill -f DistEntry')
