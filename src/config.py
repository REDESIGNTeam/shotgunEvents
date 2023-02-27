import json
import os
from six.moves import configparser
import boto3
import requests
from exceptions import ConfigError

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None


def _getSgSecret(secretName):
    # Create a Secrets Manager client
    secSession = boto3.session.Session()
    client = secSession.client(service_name="secretsmanager", region_name=_getInstanceRegion())
    getSecretValueResponse = client.get_secret_value(SecretId=secretName)

    formatted = json.loads(getSecretValueResponse["SecretString"])
    return formatted[secretName]


def _getSgHost():
    # Create a Secrets Manager client
    secSession = boto3.session.Session()
    ssmClient = secSession.client(service_name="ssm", region_name=_getInstanceRegion())
    response = ssmClient.get_parameter(Name="AA-ShotgunHost")
    shotgunHost = response["Parameter"]["Value"]

    return shotgunHost


def _getInstanceRegion():
    """
    Get ec2 region from the instance metadata by making an HTTP request.

    AWS doc - https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html

    :return: str.
    """
    instanceIdentityUrl = "http://169.254.169.254/latest/dynamic/instance-identity/document"
    response = requests.get(instanceIdentityUrl)
    jsonResponse = response.json()

    return jsonResponse.get("region")


class Config(configparser.SafeConfigParser):
    def __init__(self, path):
        configparser.SafeConfigParser.__init__(self, os.environ)
        self.read(path)

    def getShotgunURL(self):
        if self.has_option("shotgun", "server"):
            server = self.get("shotgun", "server")
            if server:
                return server
        return _getSgHost()

    def getEngineScriptName(self):
        return self.get("shotgun", "name")

    def getEngineScriptKey(self):
        if self.has_option("shotgun", "server"):
            key = self.get("shotgun", "key")
            if key:
                return key
            return _getSgSecret(self.getEngineScriptName())

    def getEngineProxyServer(self):
        try:
            proxyServer = self.get("shotgun", "proxy_server").strip()
            if not proxyServer:
                return None
            return proxyServer
        except configparser.NoOptionError:
            return None

    def getEventIdFile(self):
        return self.get("daemon", "eventIdFile")

    def getPluginPaths(self):
        return [s.strip() for s in self.get("plugins", "paths").split(",")]

    def getSMTPServer(self):
        return self.get("emails", "server")

    def getSMTPPort(self):
        if self.has_option("emails", "port"):
            return self.getint("emails", "port")
        return 25

    def getFromAddr(self):
        return self.get("emails", "from")

    def getToAddrs(self):
        return [s.strip() for s in self.get("emails", "to").split(",")]

    def getEmailSubject(self):
        return self.get("emails", "subject")

    def getEmailUsername(self):
        if self.has_option("emails", "username"):
            return self.get("emails", "username")
        return None

    def getEmailPassword(self):
        if self.has_option("emails", "password"):
            return self.get("emails", "password")
        return None

    def getSecureSMTP(self):
        if self.has_option("emails", "useTLS"):
            return self.getboolean("emails", "useTLS") or False
        return False

    def getLogMode(self):
        return self.getint("daemon", "logMode")

    def getLogLevel(self):
        return self.getint("daemon", "logging")

    def getMaxEventBatchSize(self):
        if self.has_option("daemon", "max_event_batch_size"):
            return self.getint("daemon", "max_event_batch_size")
        return 500

    def getLogFile(self, filename=None):
        if filename is None:
            if self.has_option("daemon", "logFile"):
                filename = self.get("daemon", "logFile")
            else:
                raise ConfigError("The config file has no logFile option.")

        if self.has_option("daemon", "logPath"):
            path = self.get("daemon", "logPath")

            if not os.path.exists(path):
                os.makedirs(path)
            elif not os.path.isdir(path):
                raise ConfigError(
                    "The logPath value in the config should point to a directory."
                )

            path = os.path.join(path, filename)

        else:
            path = filename

        return path

    def getTimingLogFile(self):
        if (
            not self.has_option("daemon", "timing_log")
            or self.get("daemon", "timing_log") != "on"
        ):
            return None

        return self.getLogFile() + ".timing"

    def getSentryDsn(self):
        if self.has_option('sentry', 'sentry_dsn') and self.get('sentry', 'sentry_dsn'):
            return self.get('sentry', 'sentry_dsn')
        return None

    def isBatchMode(self):
        if self.has_option("daemon", "batch_plugin") and self.get("daemon", "batch_plugin") == "on":
            return True
        return False
