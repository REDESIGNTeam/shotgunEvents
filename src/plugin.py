import importlib
import datetime
import logging
import os
import traceback
import shotgun_api3 as sg
from registrar import Registrar
from callback import Callback, BatchCallback
import utilities


class Plugin(object):
    """
    The plugin class represents a file on disk which contains one or more
    callbacks.
    """

    def __init__(self, engine, path):
        """
        @param engine: The engine that instantiated this plugin.
        @type engine: L{Engine}
        @param path: The path of the plugin file to load.
        @type path: I{str}

        @raise ValueError: If the path to the plugin is not a valid file.
        """
        self._engine = engine
        self._path = path

        if not os.path.isfile(path):
            raise ValueError("The path to the plugin is not a valid file - %s." % path)

        self._pluginName = os.path.splitext(os.path.split(self._path)[1])[0]
        self._active = True
        self._callbacks = []
        self._mtime = None
        self._lastEventId = None
        self._backlog = {}

        # Setup the plugin's logger
        self.logger = logging.getLogger("plugin." + self.getName())
        self.logger.config = self._engine.config
        self._engine.setEmailsOnLogger(self.logger, True)
        self.logger.setLevel(self._engine.config.getLogLevel())
        if self._engine.config.getLogMode() == 1:
            utilities.setFilePathOnLogger(
                self.logger, self._engine.config.getLogFile("plugin." + self.getName())
            )

    def getName(self):
        return self._pluginName

    def setState(self, state):
        if isinstance(state, int):
            self._lastEventId = state
        elif isinstance(state, tuple):
            self._lastEventId, self._backlog = state
        else:
            raise ValueError("Unknown state type: %s." % type(state))

    def getState(self):
        return (self._lastEventId, self._backlog)

    def getNextUnprocessedEventId(self):
        if self._lastEventId:
            nextId = self._lastEventId + 1
        else:
            nextId = None

        now = datetime.datetime.now()
        for k in list(self._backlog):
            v = self._backlog[k]
            if v < now:
                self.logger.warning("Timeout elapsed on backlog event id %d.", k)
                del self._backlog[k]
            elif nextId is None or k < nextId:
                nextId = k

        return nextId

    def isActive(self):
        """
        Is the current plugin active. Should it's callbacks be run?

        @return: True if this plugin's callbacks should be run, False otherwise.
        @rtype: I{bool}
        """
        return self._active

    def setEmails(self, *emails):
        """
        Set the email addresses to whom this plugin should send errors.

        @param emails: See L{LogFactory.getLogger}'s emails argument for info.
        @type emails: A I{list}/I{tuple} of email addresses or I{bool}.
        """
        self._engine.setEmailsOnLogger(self.logger, emails)

    def load(self):
        """
        Load/Reload the plugin and all its callbacks.

        If a plugin has never been loaded it will be loaded normally. If the
        plugin has been loaded before it will be reloaded only if the file has
        been modified on disk. In this event callbacks will all be cleared and
        reloaded.

        General behavior:
        - Try to load the source of the plugin.
        - Try to find a function called registerCallbacks in the file.
        - Try to run the registration function.

        At every step along the way, if any error occurs the whole plugin will
        be deactivated and the function will return.
        """
        # Check file mtime
        mtime = os.path.getmtime(self._path)
        if self._mtime is None:
            self._engine.log.info("Loading plugin at %s" % self._path)
        elif self._mtime < mtime:
            self._engine.log.info("Reloading plugin at %s" % self._path)
        else:
            # The mtime of file is equal or older. We don't need to do anything.
            return

        # Reset values
        self._mtime = mtime
        self._callbacks = []
        self._active = True

        try:
            loader = importlib.machinery.SourceFileLoader(self._pluginName, self._path)
            spec = importlib.machinery.ModuleSpec(self._pluginName, loader, origin=self._path)
            plugin = importlib.util.module_from_spec(spec)
            loader.exec_module(plugin)
        except:
            self._active = False
            self.logger.error(
                "Could not load the plugin at %s.\n\n%s",
                self._path,
                traceback.format_exc(),
            )
            return

        regFunc = getattr(plugin, "registerCallbacks", None)
        if callable(regFunc):
            try:
                regFunc(Registrar(self))
            except:
                self._engine.log.critical(
                    "Error running register callback function from plugin at %s.\n\n%s",
                    self._path,
                    traceback.format_exc(),
                )
                self._active = False
        else:
            self._engine.log.critical(
                "Did not find a registerCallbacks function in plugin at %s.", self._path
            )
            self._active = False

    def registerCallback(
        self,
        sgScriptName,
        sgScriptKey,
        callback,
        matchEvents=None,
        args=None,
        stopOnError=False,
    ):
        """
        Register a callback in the plugin.
        """
        sgConnection = sg.Shotgun(
            self._engine.config.getShotgunURL(),
            sgScriptName,
            sgScriptKey,
            http_proxy=self._engine.config.getEngineProxyServer(),
        )
        self._callbacks.append(
            Callback(
                callback,
                self,
                self._engine,
                sgConnection,
                matchEvents,
                args,
                stopOnError,
            )
        )

    def process(self, event):
        if event["id"] in self._backlog:
            if self._process(event):
                self.logger.info("Processed id %d from backlog." % event["id"])
                del self._backlog[event["id"]]
                self._updateLastEventId(event)
        elif self._lastEventId is not None and event["id"] <= self._lastEventId:
            msg = "Event %d is too old. Last event processed was (%d)."
            self.logger.debug(msg, event["id"], self._lastEventId)
        else:
            if self._process(event):
                self._updateLastEventId(event)

        return self._active

    def _process(self, event):
        for callback in self:
            if callback.isActive():
                if callback.canProcess(event):
                    msg = "Dispatching event %d to callback %s."
                    self.logger.debug(msg, event["id"], str(callback))
                    if not callback.process(event):
                        # A callback in the plugin failed. Deactivate the whole
                        # plugin.
                        self._active = False
                        break
            else:
                msg = "Skipping inactive callback %s in plugin."
                self.logger.debug(msg, str(callback))

        return self._active

    def _updateLastEventId(self, event):
        BACKLOG_TIMEOUT = (
            5  # time in minutes after which we consider a pending event won't happen
        )
        if self._lastEventId is not None and event["id"] > self._lastEventId + 1:
            event_date = event["created_at"].replace(tzinfo=None)
            if datetime.datetime.now() > (
                event_date + datetime.timedelta(minutes=BACKLOG_TIMEOUT)
            ):
                # the event we've just processed happened more than BACKLOG_TIMEOUT minutes ago so any event
                # with a lower id should have shown up in the EventLog by now if it actually happened
                if event["id"] == self._lastEventId + 2:
                    self.logger.info(
                        "Event %d never happened - ignoring.", self._lastEventId + 1
                    )
                else:
                    self.logger.info(
                        "Events %d-%d never happened - ignoring.",
                        self._lastEventId + 1,
                        event["id"] - 1,
                    )
            else:
                # in this case, we want to add the missing events to the backlog as they could show up in the
                # EventLog within BACKLOG_TIMEOUT minutes, during which we'll keep asking for the same range
                # them to show up until they expire
                expiration = datetime.datetime.now() + datetime.timedelta(
                    minutes=BACKLOG_TIMEOUT
                )
                for skippedId in range(self._lastEventId + 1, event["id"]):
                    self.logger.info("Adding event id %d to backlog.", skippedId)
                    self._backlog[skippedId] = expiration
        self._lastEventId = event["id"]

    def __iter__(self):
        """
        A plugin is iterable and will iterate over all its L{Callback} objects.
        """
        return self._callbacks.__iter__()

    def __str__(self):
        """
        Provide the name of the plugin when it is cast as string.

        @return: The name of the plugin.
        @rtype: I{str}
        """
        return self.getName()


class BatchPlugin(Plugin):

    def registerCallback(
        self,
        sgScriptName,
        sgScriptKey,
        callback,
        matchEvents=None,
        args=None,
        stopOnError=False,
    ):
        """
        Register a callback in the plugin.
        """
        sgConnection = sg.Shotgun(
            self._engine.config.getShotgunURL(),
            sgScriptName,
            sgScriptKey,
            http_proxy=self._engine.config.getEngineProxyServer(),
        )
        self._callbacks.append(
            BatchCallback(
                callback,
                self,
                self._engine,
                sgConnection,
                matchEvents,
                args,
                stopOnError,
            )
        )

    def process(self, events):
        events_in_backlog = [event for event in events if event["id"] in self._backlog]
        old_events = [event for event in events if self._lastEventId is not None and event["id"] <= self._lastEventId]
        if events_in_backlog:
            if self._process(events):
                for event in events_in_backlog: # TODO: Remove for loop
                    self.logger.info("Processed id %d from backlog." % event["id"])
                    del self._backlog[event["id"]]
                    self._updateLastEventId(event)
        elif old_events:
            msg = "Events %s are too old. Last event processed was (%d)."
            event_ids = ', '.join(map(lambda event: str(event["id"]), old_events))
            self.logger.debug(msg, event_ids, self._lastEventId)
        else:
            if self._process(events):
                for event in events: # TODO: Remove for loop
                    self._updateLastEventId(event)

        return self._active

    def _process(self, events):
        for callback in self:
            if callback.isActive():
                events_to_process = [event for event in events if callback.canProcess(event)]
                if events_to_process:
                    msg = "Dispatching event %s to callback %s."
                    event_ids = ', '.join(map(lambda event: str(event["id"]), events_to_process))
                    self.logger.debug(msg, event_ids, str(callback))
                    if not callback.process(events_to_process):
                        # A callback in the plugin failed. Deactivate the whole
                        # plugin.
                        self._active = False
                        break
            else:
                msg = "Skipping inactive callback %s in plugin."
                self.logger.debug(msg, str(callback))
        return self._active
