class Registrar(object):
    """
    See public API docs in docs folder.
    """

    def __init__(self, plugin):
        """
        Wrap a plugin, so it can be passed to a user.
        """
        self._plugin = plugin
        self._allowed = ["logger", "setEmails", "registerCallback"]

    def getLogger(self):
        """
        Get the logger for this plugin.

        @return: The logger configured for this plugin.
        @rtype: L{logging.Logger}
        """
        # TODO: Fix this ugly protected member access
        return self.logger

    def __getattr__(self, name):
        if name in self._allowed:
            return getattr(self._plugin, name)
        raise AttributeError(
            "type object '%s' has no attribute '%s'" % (type(self).__name__, name)
        )
