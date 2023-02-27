import logging
import logging.handlers
import os
import re
import socket
import time
import traceback
import six.moves.cPickle as pickle
import shotgun_api3 as sg
from exceptions import EventDaemonError
from pluginCollection import PluginCollection, BatchPluginCollection

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None
import utilities


class Engine(object):
    """
    The engine holds the main loop of event processing.
    """

    def __init__(self, config):
        """
        """
        self.start_engine = False
        self._eventIdData = {}

        # Read/parse the config
        self.config = config

        self.setSentryNotification()

        # Get config values
        self._is_batch_mode = self.config.isBatchMode()
        if self._is_batch_mode:
            self._pluginCollections = [
                BatchPluginCollection(self, s) for s in self.config.getPluginPaths()
            ]
        else:
            self._pluginCollections = [
                PluginCollection(self, s) for s in self.config.getPluginPaths()
            ]
        self._sg = sg.Shotgun(
            self.config.getShotgunURL(),
            self.config.getEngineScriptName(),
            self.config.getEngineScriptKey(),
            http_proxy=self.config.getEngineProxyServer(),
        )
        self._maxConnRetries = self.config.getint("daemon", "max_conn_retries")
        self._connRetrySleep = self.config.getint("daemon", "conn_retry_sleep")
        self._fetchInterval = self.config.getint("daemon", "fetch_interval")
        self._useSessionUUId = self.config.getboolean("shotgun", "use_session_uuid")

        # Setup the loggers for the main engine
        if self.config.getLogMode() == 0:
            # Set the root logger for file output.
            rootLogger = logging.getLogger()
            rootLogger.config = self.config
            utilities.setFilePathOnLogger(rootLogger, self.config.getLogFile())

            # Set the engine logger for email output.
            self.log = logging.getLogger("engine")
            self.setEmailsOnLogger(self.log, True)
        else:
            # Set the engine logger for file and email output.
            self.log = logging.getLogger("engine")
            self.log.config = self.config
            utilities.setFilePathOnLogger(self.log, self.config.getLogFile())
            self.setEmailsOnLogger(self.log, True)

        self.log.setLevel(self.config.getLogLevel())

        # Setup the timing log file
        timing_log_filename = self.config.getTimingLogFile()
        if timing_log_filename:
            self.timing_logger = logging.getLogger("timing")
            self.timing_logger.setLevel(self.config.getLogLevel())
            utilities.setFilePathOnLogger(self.timing_logger, timing_log_filename)
        else:
            self.timing_logger = None
        self.log.debug("Engine init")
        self.log.debug(self._is_batch_mode)
        super(Engine, self).__init__()

    def setEmailsOnLogger(self, logger, emails):
        # Configure the logger for email output
        utilities.removeHandlersFromLogger(logger, logging.handlers.SMTPHandler)

        if emails is False:
            return

        smtpServer = self.config.getSMTPServer()
        smtpPort = self.config.getSMTPPort()
        fromAddr = self.config.getFromAddr()
        emailSubject = self.config.getEmailSubject()
        username = self.config.getEmailUsername()
        password = self.config.getEmailPassword()
        if self.config.getSecureSMTP():
            secure = (None, None)
        else:
            secure = None

        if emails is True:
            toAddrs = self.config.getToAddrs()
        elif isinstance(emails, (list, tuple)):
            toAddrs = emails
        else:
            msg = "Argument emails should be True to use the default addresses, False to not send any emails or a list of recipient addresses. Got %s."
            raise ValueError(msg % type(emails))

        utilities.addMailHandlerToLogger(
            logger,
            (smtpServer, smtpPort),
            fromAddr,
            toAddrs,
            emailSubject,
            username,
            password,
            secure,
        )

    def setSentryNotification(self):
        sentry_dsn = self.config.getSentryDsn()
        if sentry_dsn:
            sentry_sdk.init(dsn=sentry_dsn, ignore_errors=[KeyboardInterrupt], before_send=utilities.sentry_pre_send)
            with sentry_sdk.configure_scope() as scope:
                shotgun_account_name = re.match('.*://(.*).(shotgunstudio|shotgrid.autodesk).com',
                                                self.config.getShotgunURL()).group(1)
                scope.set_tag("shotgun_account", shotgun_account_name)
                scope.level = 'fatal'

    def start(self):
        """
        Start the processing of events.

        The last processed id is loaded up from persistent storage on disk and
        the main loop is started.
        """
        self.start_engine = True
        self.log.debug("Engine start")
        self.log.debug("Starting the event processing loop.")
        # TODO: Take value from config
        socket.setdefaulttimeout(60)

        # Notify which version of shotgun api we are using
        self.log.info("Using SG Python API version %s" % sg.__version__)

        try:
            for collection in self._pluginCollections:
                collection.load()

            self._loadEventIdData()

            self.mainLoop()
        except KeyboardInterrupt:
            self.log.warning("Keyboard interrupt. Cleaning up...")
        except Exception as err:
            msg = "Crash!!!!! Unexpected error (%s) in main loop.\n\n%s"
            self.log.critical(msg, type(err), traceback.format_exc(err))

    def _loadEventIdData(self):
        """
        Load the last processed event id from the disk

        If no event has ever been processed or if the eventIdFile has been
        deleted from disk, no id will be recoverable. In this case, we will try
        contacting Shotgun to get the latest event's id, and we'll start
        processing from there.
        """
        eventIdFile = self.config.getEventIdFile()

        if eventIdFile and os.path.exists(eventIdFile):
            try:
                fh = open(eventIdFile, "rb")
                try:
                    self._eventIdData = pickle.load(fh)
                    # Provide event id info to the plugin collections. Once
                    # they've figured out what to do with it, ask them for their
                    # last processed id.
                    noStateCollections = []
                    for collection in self._pluginCollections:
                        state = self._eventIdData.get(collection.path)
                        if state:
                            collection.setState(state)
                        else:
                            noStateCollections.append(collection)

                    # If we don't have a state it means there's no match
                    # in the id file. First we'll search to see the latest id a
                    # matching plugin name has elsewhere in the id file. We do
                    # this as a fallback in case the plugins directory has been
                    # moved. If there's no match, use the latest event id
                    # in Shotgun.
                    if noStateCollections:
                        maxPluginStates = {}
                        for collection in self._eventIdData.values():
                            for pluginName, pluginState in collection.items():
                                if pluginName in maxPluginStates.keys():
                                    if pluginState[0] > maxPluginStates[pluginName][0]:
                                        maxPluginStates[pluginName] = pluginState
                                else:
                                    maxPluginStates[pluginName] = pluginState

                        lastEventId = self._getLastEventIdFromDatabase()
                        for collection in noStateCollections:
                            state = collection.getState()
                            for pluginName in state.keys():
                                if pluginName in maxPluginStates.keys():
                                    state[pluginName] = maxPluginStates[pluginName]
                                else:
                                    state[pluginName] = lastEventId
                            collection.setState(state)

                except pickle.UnpicklingError:
                    fh.close()

                    # Backwards compatibility:
                    # Reopen the file to try to read an old-style int
                    fh = open(eventIdFile, "rb")
                    line = fh.readline().strip()
                    if line.isdigit():
                        # The _loadEventIdData got an old-style id file containing a single
                        # int which is the last id properly processed.
                        lastEventId = int(line)
                        self.log.debug(
                            "Read last event id (%d) from file.", lastEventId
                        )
                        for collection in self._pluginCollections:
                            collection.setState(lastEventId)
                fh.close()
            except OSError as err:
                raise EventDaemonError(
                    "Could not load event id from file.\n\n%s"
                    % traceback.format_exc(err)
                )
        else:
            # No id file?
            # Get the event data from the database.
            lastEventId = self._getLastEventIdFromDatabase()
            if lastEventId:
                for collection in self._pluginCollections:
                    collection.setState(lastEventId)

            self._saveEventIdData()

    def _getLastEventIdFromDatabase(self):
        conn_attempts = 0
        lastEventId = None
        while lastEventId is None:
            order = [{"column": "id", "direction": "desc"}]
            for conn_attempts in range(self._maxConnRetries):
                try:
                    result = self._sg.find_one(
                        "EventLogEntry", filters=[], fields=["id"], order=order
                    )
                except (sg.ProtocolError, sg.ResponseError, socket.error) as err:
                    self._writeConnectionAttemptLog(conn_attempts, str(err))
                except Exception as err:
                    msg = "Unknown error: %s" % str(err)
                    self._writeConnectionAttemptLog(conn_attempts, msg)
                else:
                    if result:
                        lastEventId = result["id"]
                    self.log.info("Last event id (%d) from the SG database.", lastEventId)

        return lastEventId

    def mainLoop(self):
        """
        Run the event processing loop.

        General behavior:
        - Load plugins from disk - see L{load} method.
        - Get new events from Shotgun
        - Loop through events
        - Loop through each plugin
        - Loop through each callback
        - Send the callback an event
        - Once all callbacks are done in all plugins, save the eventId
        - Go to the next event
        - Once all events are processed, wait for the defined fetch interval time and start over.

        Caveats:
        - If a plugin is deemed "inactive" (an error occured during
          registration), skip it.
        - If a callback is deemed "inactive" (an error occured during callback
          execution), skip it.
        - Each time through the loop, if the pidFile is gone, stop.
        """
        if self.start_engine:
            # Process events
            events = self._getNewEvents()
            if self._is_batch_mode:
                for collection in self._pluginCollections:
                    if events:
                        collection.process(events)
            else:
                for event in events:
                    for collection in self._pluginCollections:
                        collection.process(event)
            if events:
                self._saveEventIdData()

            # if we're lagging behind Shotgun, we received a full batch of events
            # skip the sleep() call in this case
            if len(events) < self.config.getMaxEventBatchSize():
                time.sleep(self._fetchInterval)

            # Reload plugins
            for collection in self._pluginCollections:
                collection.load()

            # Make sure that newly loaded events have proper state.
            if events:
                self._loadEventIdData()



    def stop(self):
        self.log.debug("Shutting down event processing loop.")
        self.start_engine = False

    def _getNewEvents(self):
        """
        Fetch new events from Shotgun.

        @return: Recent events that need to be processed by the engine.
        @rtype: I{list} of Shotgun event dictionaries.
        """
        nextEventId = None
        for newId in [
            coll.getNextUnprocessedEventId() for coll in self._pluginCollections
        ]:
            if newId is not None and (nextEventId is None or newId < nextEventId):
                nextEventId = newId
        if nextEventId is not None:
            filters = [["id", "greater_than", int(nextEventId) - 1]]
            fields = [
                "id",
                "event_type",
                "attribute_name",
                "meta",
                "entity",
                "user",
                "project",
                "session_uuid",
                "created_at",
            ]
            order = [{"column": "id", "direction": "asc"}]

            for conn_attempts in range(self._maxConnRetries):
                try:
                    events = self._sg.find(
                        "EventLogEntry",
                        filters,
                        fields,
                        order,
                        limit=self.config.getMaxEventBatchSize(),
                    )
                    if events:
                        self.log.debug(
                            "Got %d events: %d to %d.",
                            len(events),
                            events[0]["id"],
                            events[-1]["id"],
                        )
                    return events
                except (sg.ProtocolError, sg.ResponseError, socket.error) as err:
                    self._writeConnectionAttemptLog(conn_attempts, str(err))
                except Exception as err:
                    msg = "Unknown error: %s" % str(err)
                    self._writeConnectionAttemptLog(conn_attempts, msg)

        return []

    def _saveEventIdData(self):
        """
        Save an event Id to persistant storage.

        Next time the engine is started it will try to read the event id from
        this location to know at which event it should start processing.
        """
        eventIdFile = self.config.getEventIdFile()

        if eventIdFile is not None:
            for collection in self._pluginCollections:
                self._eventIdData[collection.path] = collection.getState()

            for colPath, state in self._eventIdData.items():
                if state:
                    try:
                        with open(eventIdFile, "wb") as fh:
                            # Use protocol 2 so it can also be loaded in Python 2
                            pickle.dump(self._eventIdData, fh, protocol=2)
                    except OSError as err:
                        self.log.error(
                            "Can not write event id data to %s.\n\n%s",
                            eventIdFile,
                            traceback.format_exc(err),
                        )
                    break
            else:
                self.log.warning("No state was found. Not saving to disk.")

    def _checkConnectionAttempts(self, conn_attempts, msg):
        conn_attempts += 1
        if conn_attempts == self._maxConnRetries:
            self.log.error(
                "Unable to connect to SG (attempt %s of %s): %s",
                conn_attempts,
                self._maxConnRetries,
                msg,
            )
            conn_attempts = 0
            time.sleep(self._connRetrySleep)
        else:
            self.log.warning(
                "Unable to connect to SG (attempt %s of %s): %s",
                conn_attempts,
                self._maxConnRetries,
                msg,
            )
        return conn_attempts

    def _writeConnectionAttemptLog(self, conn_attempts, msg):
        if conn_attempts == self._maxConnRetries - 1:
            self.log.error("Unable to connect to SG (attempt %s of %s): %s", conn_attempts,
                           self._maxConnRetries, str(msg),
                           )
            time.sleep(self._connRetrySleep)
        else:
            self.log.warning("Unable to connect to SG (attempt %s of %s): %s", conn_attempts,
                             self._maxConnRetries, str(msg))

