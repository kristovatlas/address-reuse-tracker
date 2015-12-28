####################
# EXTERNAL IMPORTS #
####################

import syslog
import time     #timestamp
import datetime #timestamp
import sys

#############
# CONSTANTS #
#############

STATUS_LOG_NAME = 'address-reuse.log' # TODO move to config file

#############
# FUNCTIONS #
#############

def log_alert(message):
    timestamp = get_current_timestamp()
    msg_with_stamp = "[%s]: %s" % (timestamp, message)
    syslog.syslog(syslog.LOG_ALERT, msg_with_stamp)

def print_and_log_alert(message):
    print(message)
    log_alert(message)

def log_and_die(message):
    log_alert(message)
    sys.exit(message)

def get_current_timestamp():
    return datetime.datetime.fromtimestamp(time.time()).strftime(
        '%Y-%m-%d %H:%M:%S')
        
def log_status(message):
    timestamp = get_current_timestamp()
    with open(STATUS_LOG_NAME, "a") as logfile:
        logfile.write("[%s]: %s\n" % (timestamp, message))
