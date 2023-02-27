import datetime
import logging
import logging.handlers
import pprint
import sys
import traceback
import shotgun_api3 as sg
from shotgun_api3.lib.sgtimezone import SgTimezone
try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None


class Callback(object):
    """
    A part of a plugin that can be called to process a Shotgun event.
    """

    def __init__(
        self,
        callback,
        plugin,
        engine,
        shotgun,
        matchEvents=None,
        args=None,
        stopOnError=False,
    ):
        """
        @param callback: The function to run when a Shotgun event occurs.
        @type callback: A function object.
        @param engine: The engine that will dispatch to this callback.
        @type engine: L{Engine}.
        @param shotgun: The Shotgun instance that will be used to communicate
            with your Shotgun server.
        @type shotgun: L{sg.Shotgun}
        @param matchEvents: The event filter to match events against before invoking callback.
        @type matchEvents: dict
        @param args: Any datastructure you would like to be passed to your
            callback function. Defaults to None.
        @type args: Any object.

        @raise TypeError: If the callback is not a callable object.
        """
        if not callable(callback):
            raise TypeError(
                "The callback must be a callable object (function, method or callable class instance)."
            )

        self._name = None
        self._shotgun = shotgun
        self._plugin = plugin
        self._callback = callback
        self._engine = engine
        self._logger = None
        self._matchEvents = matchEvents
        self._args = args
        self._stopOnError = stopOnError
        self._active = True

        # Find a name for this object
        if hasattr(callback, "__name__"):
            self._name = callback.__name__
        elif hasattr(callback, "__class__") and hasattr(callback, "__call__"):
            self._name = "%s_%s" % (callback.__class__.__name__, hex(id(callback)))
        else:
            raise ValueError(
                "registerCallback should be called with a function or a callable object instance as callback argument."
            )
        self._sg_time_zone = SgTimezone()
        # TODO: Get rid of this protected member access
        self._logger = logging.getLogger(plugin.logger.name + "." + self._name)
        self._logger.config = self._engine.config
        self.is_batch_plugin = False

    def canProcess(self, event):
        if not self._matchEvents:
            return True

        if "*" in self._matchEvents:
            eventType = "*"
        else:
            eventType = event["event_type"]
            if eventType not in self._matchEvents:
                return False

        attributes = self._matchEvents[eventType]

        if attributes is None or "*" in attributes:
            return True

        if event["attribute_name"] and event["attribute_name"] in attributes:
            return True

        return False

    def process(self, event):
        """
        Process an event with the callback object supplied on initialization.

        If an error occurs, it will be logged appropriately and the callback
        will be deactivated.

        @param event: The Shotgun event to process.
        @type event: I{dict}
        """
        # set session_uuid for UI updates
        if self._engine._useSessionUUId:
            self._shotgun.set_session_uuid(event["session_uuid"])

        if self._engine.timing_logger:
            startTime = datetime.datetime.now(self._sg_time_zone.local)

        try:
            self._callback(self._shotgun, self._logger, event, self._args)
            error = False
        except:
            error = True

            # Get the local variables of the frame of our plugin
            tb = sys.exc_info()[2]
            stack = []
            while tb:
                stack.append(tb.tb_frame)
                tb = tb.tb_next

            msg = "An error occurred processing an event.\n\n%s\n\nLocal variables at outer most frame in plugin:\n\n%s"
            self._logger.critical(
                msg, traceback.format_exc(), pprint.pformat(stack[1].f_locals)
            )

            if sentry_sdk is not None:
                _senExtra = {'plugin_name': self._plugin.getName(),
                              'event_id': str(event['id']),
                              'stop_on_error': str(self._stopOnError)}
                if self._stopOnError:
                    _senExtra['level'] = 'error'
                    msg = 'An error occurred processing an event.'
                    msg += '\nStopOnError is True, so skipping the plugin from daemon.'
                    msg += '\n\n%s\n\nLocal variables at outer most frame in plugin:\n\n%s'
                else:
                    _senExtra['level'] = 'warning'
                self._logger.critical(msg, traceback.format_exc(), pprint.pformat(stack[1].f_locals), extra=_senExtra)
            else:
                self._logger.critical(msg, traceback.format_exc(), pprint.pformat(stack[1].f_locals))

            if self._stopOnError:
                self._active = False

        if self._engine.timing_logger:
            callbackName = self._logger.name.replace("plugin.", "")
            endTime = datetime.datetime.now(self._sg_time_zone.local)
            duration = self._prettyTimeDeltaFormat(endTime - startTime)
            delay = self._prettyTimeDeltaFormat(startTime - event["created_at"])
            msgFormat = "event_id=%d created_at=%s callback=%s start=%s end=%s duration=%s error=%s delay=%s"
            data = [
                event["id"],
                event["created_at"].isoformat(),
                callbackName,
                startTime.isoformat(),
                endTime.isoformat(),
                duration,
                str(error),
                delay,
            ]
            self._engine.timing_logger.info(msgFormat, *data)

        return self._active

    def _prettyTimeDeltaFormat(self, time_delta):
        days, remainder = divmod(time_delta.total_seconds(), 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        return "%02d:%02d:%02d:%02d.%06d" % (
            days,
            hours,
            minutes,
            seconds,
            time_delta.microseconds,
        )

    def isActive(self):
        """
        Check if this callback is active, i.e. if events should be passed to it
        for processing.

        @return: True if this callback should process events, False otherwise.
        @rtype: I{bool}
        """
        return self._active

    def __str__(self):
        """
        The name of the callback.

        @return: The name of the callback
        @rtype: I{str}
        """
        return self._name


class BatchCallback(Callback):

    def process(self, events):
        # set session_uuid for UI updates
        if self._engine._useSessionUUId and events:
            self._shotgun.set_session_uuid(events[-1]["session_uuid"])

        if self._engine.timing_logger:
            startTime = datetime.datetime.now(self._sg_time_zone.local)

        try:
            self._callback(self._shotgun, self._logger, events, self._args)
            error = False
        except:
            error = True

            # Get the local variables of the frame of our plugin
            tb = sys.exc_info()[2]
            stack = []
            while tb:
                stack.append(tb.tb_frame)
                tb = tb.tb_next

            msg = "An error occured processing an event.\n\n%s\n\nLocal variables at outer most frame in plugin:\n\n%s"
            self._logger.critical(
                msg, traceback.format_exc(), pprint.pformat(stack[1].f_locals)
            )

            if sentry_sdk is not None:
                eventIds = ', '.join(map(lambda event: str(event["id"]), events))
                _senExtra = {'plugin_name': self._plugin.getName(),
                              'event_id': str(eventIds),
                              'stop_on_error': str(self._stopOnError)}
                if self._stopOnError:
                    _senExtra['level'] = 'error'
                    msg = 'An error occured processing an event.'
                    msg += '\nStopOnError is True, so skipping the plugin from daemon.'
                    msg += '\n\n%s\n\nLocal variables at outer most frame in plugin:\n\n%s'
                else:
                    _senExtra['level'] = 'warning'
                self._logger.critical(msg, traceback.format_exc(), pprint.pformat(stack[1].f_locals), extra=_senExtra)
            else:
                self._logger.critical(msg, traceback.format_exc(), pprint.pformat(stack[1].f_locals))

            if self._stopOnError:
                self._active = False

        if self._engine.timing_logger:
            callback_name = self._logger.name.replace("plugin.", "")
            endTime = datetime.datetime.now(self._sg_time_zone.local)
            duration = self._prettyTimeDeltaFormat(endTime - startTime)
            for event in events:
                delay = self._prettyTimeDeltaFormat(startTime - event["created_at"])
                msgFormat = "event_id=%d created_at=%s callback=%s start=%s end=%s duration=%s error=%s delay=%s"
                data = [
                    event["id"],
                    event["created_at"].isoformat(),
                    callback_name,
                    startTime.isoformat(),
                    endTime.isoformat(),
                    duration,
                    str(error),
                    delay,
                ]
                self._engine.timing_logger.info(msgFormat, *data)
        return self._active
