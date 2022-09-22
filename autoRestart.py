import os
import logging
import subprocess

pid_file = '/usr/local/shotgun/logs/shotgunEventDaemon/shotgunEventDaemon.pid'
py_file = '/usr/local/shotgun/shotgunEvents/src/shotgunEventDaemon.py'

log_path = '%s/autoRestart' % os.path.dirname(pid_file)
if not os.path.exists(os.path.dirname(log_path)):
    os.makedirs(os.path.dirname(log_path))
fh = logging.FileHandler(log_path)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
logger = logging.getLogger('restart')
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)


def check_pid(pid):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def check_service():
    logger.info('checking if service is running')
    if os.path.exists(pid_file):
        logger.info('pid file exists')
        with open(pid_file, 'r') as _file:
            pid = _file.read()
        pid = int(pid)
        logger.info('event server pid - %s' % pid)
        if not check_pid(pid):
            logger.info('process with pid - %s is not running, restarting server' % pid)
            subprocess.call(['python3', py_file, 'restart'])
    else:
        logger.info('pid file does not exists, restarting server')
        subprocess.call(['python3', py_file, 'restart'])


if __name__ == '__main__':
    check_service()
