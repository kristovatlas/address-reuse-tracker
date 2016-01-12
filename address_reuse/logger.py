"""Functions for print and logging status."""

import syslog
import time     #timestamp
import datetime #timestamp
import sys

STATUS_LOG_NAME = 'address-reuse.log' # TODO move to config file

def log_alert(message):
    """Append an alert message to log file."""
    timestamp = get_current_timestamp()
    msg_with_stamp = "[%s]: %s" % (timestamp, message)
    syslog.syslog(syslog.LOG_ALERT, msg_with_stamp)

def print_and_log_alert(message):
    """Print alert message to stdout and append it to a log file."""
    print message
    log_alert(message)

def log_and_die(message):
    """Append a message to log file and kill the current process."""
    log_alert(message)
    sys.exit(message)

def get_current_timestamp():
    """Get the current system time in human-readable format."""
    return datetime.datetime.fromtimestamp(time.time()).strftime(
        '%Y-%m-%d %H:%M:%S')

def log_status(message):
    """Append a status message to a log file."""
    timestamp = get_current_timestamp()
    with open(STATUS_LOG_NAME, "a") as logfile:
        logfile.write("[%s]: %s\n" % (timestamp, message))
