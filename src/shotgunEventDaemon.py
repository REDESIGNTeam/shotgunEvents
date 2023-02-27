import os
import sys
import logging
from config import Config
from utilities import getConfigPath
from writeServiceFile import writeServiceFile
from daemonizer import GracefulKiller


def main():
    """
    """
    action = None
    if len(sys.argv) > 1:
        action = sys.argv[1]

    if action:
        if sys.platform == "win32":
            daemon = GracefulKiller()
            # Find the function to call on the daemon and call it
            if action in ["start-service", "start", "restart", "foreground"]:
                daemon.start()
                return 0
            elif action == "stop":
                daemon.stop()
                return 0
        else:
            config = Config(getConfigPath())
            service_name = config.get("daemon", "service_name")
            if action == "start-service":
                status = writeServiceFile(service_name)
                if status:
                    # If file was created, reload daemon
                    os.system("systemctl daemon-reload")
                os.system('systemctl start %s' % service_name)
                return 0
            elif action in ["start", "stop", "restart"]:
                os.system('systemctl %s %s' % (action, service_name))
                return 0
            elif action == "foreground":
                # Setup the stdout logger
                handler = logging.StreamHandler()
                handler.setFormatter(
                    logging.Formatter("%(levelname)s:%(name)s:%(message)s")
                )
                logging.getLogger().addHandler(handler)
                daemon = GracefulKiller()
                daemon.start()
                return 0
        print("Unknown command: %s" % action)

    print("usage: %s start-service|start|stop|restart|foreground" % sys.argv[0])
    return 2


if __name__ == '__main__':
    sys.exit(main())
