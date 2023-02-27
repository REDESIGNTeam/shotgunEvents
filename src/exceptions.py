class EventDaemonError(Exception):
    """
    Base error for the Shotgun event system.
    """

    pass


class ConfigError(EventDaemonError):
    """
    Used when an error is detected in the config file.
    """

    pass
