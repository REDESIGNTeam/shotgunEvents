import logging
import sys
import os
from customSmtpHandler import CustomSMTPHandler
from exceptions import EventDaemonError
EMAIL_FORMAT_STRING = """Time: %(asctime)s
Logger: %(name)s
Path: %(pathname)s
Function: %(funcName)s
Line: %(lineno)d

%(message)s"""


def setFilePathOnLogger(logger, path):
    # Remove any previous handler.
    removeHandlersFromLogger(logger, logging.handlers.TimedRotatingFileHandler)

    # Add the file handler
    handler = logging.handlers.TimedRotatingFileHandler(
        path, "midnight", backupCount=10
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(handler)


def removeHandlersFromLogger(logger, handlerTypes=None):
    """
    Remove all handlers or handlers of a specified type from a logger.

    @param logger: The logger who's handlers should be processed.
    @type logger: A logging.Logger object
    @param handlerTypes: A type of handler or list/tuple of types of handlers
        that should be removed from the logger. If I{None}, all handlers are
        removed.
    @type handlerTypes: L{None}, a logging.Handler subclass or
        I{list}/I{tuple} of logging.Handler subclasses.
    """
    for handler in logger.handlers:
        if handlerTypes is None or isinstance(handler, handlerTypes):
            logger.removeHandler(handler)


def addMailHandlerToLogger(
    logger,
    smtpServer,
    fromAddr,
    toAddrs,
    emailSubject,
    username=None,
    password=None,
    secure=None,
):
    """
    Configure a logger with a handler that sends emails to specified
    addresses.

    The format of the email is defined by L{LogFactory.EMAIL_FORMAT_STRING}.

    @note: Any SMTPHandler already connected to the logger will be removed.

    @param logger: The logger to configure
    @type logger: A logging.Logger instance
    @param toAddrs: The addresses to send the email to.
    @type toAddrs: A list of email addresses that will be passed on to the
        SMTPHandler.
    """
    if smtpServer and fromAddr and toAddrs and emailSubject:
        mailHandler = CustomSMTPHandler(
            smtpServer, fromAddr, toAddrs, emailSubject, (username, password), secure
        )
        mailHandler.setLevel(logging.ERROR)
        mailFormatter = logging.Formatter(EMAIL_FORMAT_STRING)
        mailHandler.setFormatter(mailFormatter)

        logger.addHandler(mailHandler)


def sentry_pre_send(event, hint):
    if 'level' in event['extra']:
        event['level'] = event['extra']['level']
        del(event['extra']['level'])

    sentry_tags = ['plugin_name', 'stop_on_error', 'event_id']
    for _sentry_tag in sentry_tags:
        if _sentry_tag in event['extra']:
            if 'tags' not in event:
                event['tags'] = {}
            event['tags'][_sentry_tag] = event['extra'][_sentry_tag]
            del(event['extra'][_sentry_tag])

    return event


def getConfigPath():
    """
    Get the path of the shotgunEventDaemon configuration file.
    """
    paths = ["/etc", os.path.dirname(__file__)]

    # Get the current path of the daemon script
    scriptPath = sys.argv[0]
    if scriptPath != "" and scriptPath != "-c":
        # Make absolute path and eliminate any symlinks if any.
        scriptPath = os.path.abspath(scriptPath)
        scriptPath = os.path.realpath(scriptPath)

        # Add the script's directory to the paths we'll search for the config.
        paths[:0] = [os.path.dirname(scriptPath)]

    # Search for a config file.
    for path in paths:
        path = os.path.join(path, "shotgunEventDaemon.conf")
        if os.path.exists(path):
            return path

    # No config file was found
    raise EventDaemonError("Config path not found, searched %s" % ", ".join(paths))