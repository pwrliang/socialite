#!/usr/bin/python3
# -*- coding:utf-8 -*-
import os
import re
import sys
import subprocess

SOCIALITE_PREFIX = os.getenv('SOCIALITE_PREFIX')
JAVA_HOME = os.getenv('JAVA_HOME')
MPI_HOME = os.getenv("MPI_HOME")
MACHINE_FILE = SOCIALITE_PREFIX + '/conf/machines'
CLASS_PATH_LIST = []
ENTRY_CLASS_PATH = SOCIALITE_PREFIX + '/out/production/socialite'
MASTER_HOSTNAME = None
WORKER_NUM = 0
HEAP_SIZE = 4096
THREAD_NUM = 4

error_var = None

if SOCIALITE_PREFIX is None:
    error_var = "SOCIALITE_PREFIX"
elif JAVA_HOME is None:
    error_var = "JAVA_HOME"
elif MPI_HOME is None:
    error_var = "MPI_HOME"
if error_var is not None:
    raise EnvironmentError('CAN NOT FOUND ENVIRONMENT VARIABLE: %s' % error_var)

if not os.path.exists(MACHINE_FILE):
    raise IOError('Can not found machine file in %s' % MACHINE_FILE)

with open(MACHINE_FILE, 'r') as fi:
    regex = re.compile('(.+?)\s+slots=(\d+)\n?')
    for line in fi:
        match = regex.search(line)
        if match is not None:
            if MASTER_HOSTNAME is None:
                MASTER_HOSTNAME = match.groups()[0]
            WORKER_NUM += int(match.groups()[1])
    WORKER_NUM -= 1  # master is excepted


def add_class_path(root):
    file_name_list = os.listdir(root)
    for file_name in file_name_list:
        path = root + '/' + file_name
        if os.path.isdir(path):
            add_class_path(path)
        else:
            CLASS_PATH_LIST.append(path)


add_class_path(SOCIALITE_PREFIX + '/' + 'ext')
CLASS_PATH_LIST.append(ENTRY_CLASS_PATH)
class_path = ':'.join(CLASS_PATH_LIST)

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
 -Dsocialite.output.dir=gen
 -classpath %s
 socialite.async.DistEntry %s 2>&1 | tee %s""" % (
    MPI_HOME, MPI_HOME, MACHINE_FILE, JAVA_HOME,
    HEAP_SIZE, MASTER_HOSTNAME, WORKER_NUM, SOCIALITE_PREFIX + '/conf/log4j.properties',
    class_path, PROG_PATH, SOCIALITE_PREFIX + "/logs/master.log")
cmd = cmd.replace('\n', '')
os.system('pkill -f DistEntry')
os.system(cmd)
os.system('pkill -f DistEntry')
