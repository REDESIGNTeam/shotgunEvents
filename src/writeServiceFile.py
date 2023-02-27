import os
import inspect


def writeServiceFile(service_name):
    service_file_path = "/etc/systemd/system/%s" % service_name
    if not os.path.exists(service_file_path):
        current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        exe_cmd = "ExecStart=/bin/bash -c 'cd %s && python3 daemonizer.py start'" % current_dir
        working_directory = "WorkingDirectory=%s" % current_dir
        lines_to_write = ["[Unit]", "Description=Shotgun Event Daemon", "After=multi-user.target",
                          "Conflicts=getty@tty1.service", "", "[Service]", "EnvironmentFile=/etc/environment",
                          "Type=simple", working_directory, exe_cmd,
                          "StandardInput=tty-force", "", "[Install]", "WantedBy=multi-user.target"]
        with open(service_file_path, 'w') as fp:
            for idx, line in enumerate(lines_to_write):
                if idx != 0:
                    fp.write("\n%s" % line)
                else:
                    fp.write("%s" % line)
        return True
    return False
