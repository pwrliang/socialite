#!/usr/bin/env python3
import os
import sys
from common import MASTER_HOSTNAME
from common import WORKER_NUM
from common import class_path
from common import USER_NAME
from common import WORKER_HOSTNAME_LIST
from common import SOCIALITE_PREFIX

if len(sys.argv) != 2:
    print('Usage: script-name [start]/[kill]')
    exit(1)

if sys.argv[1] == 'start':
    command_line = '''
    java -Xmx6G -ea \\
    -Dsocialite.worker.num=%d \\
    -Dsocialite.output.dir=%s/gen \\
    -Dsocialite.master=%s \\
    -Dsocialite.port=50100 \\
    -Dlog4j.configuration=file:%s/conf/log4j.properties \\
    -cp %s socialite.dist.worker.WorkerNode
    ''' % (WORKER_NUM, SOCIALITE_PREFIX, MASTER_HOSTNAME, SOCIALITE_PREFIX, class_path)
    command_line = command_line.strip()

    for worker_host_name in WORKER_HOSTNAME_LIST:
        s = '''ssh -n %s@%s "sh -c 'cd %s; nohup %s > /dev/null 2>&1 &'"''' % (
        USER_NAME, worker_host_name, SOCIALITE_PREFIX, command_line)
        os.system(s)

    print("Works started!!!!!!")

    prog_args = 'dist %d %s %d'%(875713, '/home/gengl/edge.txt', 20)
    command_line = '''
    java -Xmx6G -ea \\
    -Dsocialite.worker.num=%d \\
    -Dsocialite.output.dir=%s/gen \\
    -Dsocialite.master=%s \\
    -Dsocialite.port=50100 \\
    -Dlog4j.configuration=file:%s/conf/log4j.properties \\
    -cp %s socialite.test.PageRank %s
    ''' % (WORKER_NUM, SOCIALITE_PREFIX, MASTER_HOSTNAME, SOCIALITE_PREFIX, class_path, prog_args)
    command_line = command_line.strip()
    os.system(command_line)

else:
    for worker_host_name in WORKER_HOSTNAME_LIST:
        command_line = '''ssh -n %s@%s "jps|grep WorkerNode|awk '{print $1}'|xargs kill -9 2> /dev/null"''' % (
            USER_NAME, worker_host_name)
        os.system(command_line)
