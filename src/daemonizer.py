import signal
import time
import sys
from config import Config
from utilities import getConfigPath
from engine import Engine


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        config = Config(getConfigPath())
        self._conn_sleep = int(config.get("daemon", "conn_sleep"))
        self._engine = Engine(config)

    def exit_gracefully(self, *args):
        self.kill_now = True
        self._engine.stop()

    def start(self):
        while not self.kill_now:
            if self._engine.start_engine:
                self._engine.mainLoop()
            else:
                self._engine.start()
            time.sleep(self._conn_sleep)

    def stop(self):
        self._engine.stop()
        self.exit_gracefully()


def main():
    daemon = GracefulKiller()
    daemon.start()


if __name__ == '__main__':
    if sys.platform == "linux":
        sys.exit(main())
