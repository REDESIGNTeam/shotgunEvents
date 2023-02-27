import sys
import logging
import logging.handlers
from distutils.version import StrictVersion

CURRENT_PYTHON_VERSION = StrictVersion(sys.version.split()[0])
PYTHON_27 = StrictVersion("2.7")


class CustomSMTPHandler(logging.handlers.SMTPHandler):
    """
    A custom SMTPHandler subclass that will adapt it's subject depending on the
    error severity.
    """

    LEVEL_SUBJECTS = {
        logging.ERROR: "ERROR - SG event daemon.",
        logging.CRITICAL: "CRITICAL - SG event daemon.",
    }

    def __init__(
        self, smtpServer, fromAddr, toAddrs, emailSubject, credentials=None, secure=None
    ):
        args = [smtpServer, fromAddr, toAddrs, emailSubject, credentials]
        if credentials:
            # Python 2.7 implemented the secure argument
            if CURRENT_PYTHON_VERSION >= PYTHON_27:
                args.append(secure)
            else:
                self.secure = secure

        logging.handlers.SMTPHandler.__init__(self, *args)

    def getSubject(self, record):
        subject = logging.handlers.SMTPHandler.getSubject(self, record)
        if record.levelno in self.LEVEL_SUBJECTS:
            return subject + " " + self.LEVEL_SUBJECTS[record.levelno]
        return subject

    def emit(self, record):
        """
        Emit a record.

        Format the record and send it to the specified addressees.
        """

        # Mostly copied from Python 2.7 implementation.
        try:
            import smtplib
            from email.utils import formatdate

            port = self.mailport
            if not port:
                port = smtplib.SMTP_PORT
            smtp = smtplib.SMTP(self.mailhost, port)
            msg = self.format(record)
            msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n%s" % (
                self.fromaddr,
                ",".join(self.toaddrs),
                self.getSubject(record),
                formatdate(),
                msg,
            )
            if self.username:
                if self.secure is not None:
                    smtp.ehlo()
                    smtp.starttls(*self.secure)
                    smtp.ehlo()
                smtp.login(self.username, self.password)
            smtp.sendmail(self.fromaddr, self.toaddrs, msg)
            smtp.close()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)