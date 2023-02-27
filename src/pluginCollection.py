import os
from plugin import Plugin, BatchPlugin


class PluginCollection(object):
    """
    A group of plugin files in a location on the disk.
    """

    def __init__(self, engine, path):
        if not os.path.isdir(path):
            raise ValueError("Invalid path: %s" % path)

        self._engine = engine
        self.path = path
        self._plugins = {}
        self._stateData = {}
        self.is_batch_plugin = False

    def setState(self, state):
        if isinstance(state, int):
            for plugin in self:
                plugin.setState(state)
                self._stateData[plugin.getName()] = plugin.getState()
        else:
            self._stateData = state
            for plugin in self:
                pluginState = self._stateData.get(plugin.getName())
                if pluginState:
                    plugin.setState(pluginState)

    def getState(self):
        for plugin in self:
            self._stateData[plugin.getName()] = plugin.getState()
        return self._stateData

    def getNextUnprocessedEventId(self):
        eId = None
        for plugin in self:
            if not plugin.isActive():
                continue

            newId = plugin.getNextUnprocessedEventId()
            if newId is not None and (eId is None or newId < eId):
                eId = newId
        return eId

    def process(self, event):
        for plugin in self:
            if plugin.isActive():
                plugin.process(event)
            else:
                plugin.logger.debug("Skipping: inactive.")

    def load(self):
        """
        Load plugins from disk.

        General behavior:
        - Loop on all paths.
        - Find all valid .py plugin files.
        - Loop on all plugin files.
        - For any new plugins, load them, otherwise, refresh them.
        """
        newPlugins = {}

        for basename in os.listdir(self.path):
            if not basename.endswith(".py") or basename.startswith("."):
                continue

            if basename in self._plugins:
                newPlugins[basename] = self._plugins[basename]
            else:
                newPlugins[basename] = self.get_plugin(basename)

            newPlugins[basename].load()

        self._plugins = newPlugins

    def __iter__(self):
        for basename in sorted(self._plugins.keys()):
            yield self._plugins[basename]

    def get_plugin(self, basename):
        if self.is_batch_plugin:
            return BatchPlugin(self._engine, os.path.join(self.path, basename))
        return Plugin(self._engine, os.path.join(self.path, basename))


class BatchPluginCollection(PluginCollection):
    def __init__(self, engine, path):
        super(BatchPluginCollection, self).__init__(engine, path)
        self.is_batch_plugin = True

    def process(self, events):
        for plugin in self:
            if plugin.isActive():
                plugin.process(events)
            else:
                plugin.logger.debug("Skipping: inactive.")
