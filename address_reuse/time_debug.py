####################
# EXTERNAL IMPORTS #
####################

import time
import datetime

#############
# CONSTANTS #
#############

WRITE_TO_LOG = False
DEFAULT_LOG_FILENAME = 'timer.log'

###########
# CLASSES #
###########

#A simple timer class used for profiling during development.
class Timer:
    def __init__(self, purpose = None):
        self.start = time.time()
        self.purpose = purpose
        out = "Timer started "
        if purpose is not None:
            out = out + "for " + purpose
        print_and_write_log(out)
    
    def stop(self):
        elapsed = time.time() - self.start
        out = "Timer stopped"
        if self.purpose is not None:
            out = out + " for " + self.purpose
        out = out + " after " + str(elapsed) + " seconds."
        print_and_write_log(out)
        return elapsed

#############################
# GENERAL PACKAGE FUNCTIONS #
#############################

def print_and_write_log(message):
    print(message)
    if WRITE_TO_LOG:
        timestamp = get_current_timestamp()
        with open(DEFAULT_LOG_FILENAME, "a") as logfile:
            logfile.write("[%s]: %s\n" % (timestamp, message))

def get_current_timestamp():
    return datetime.datetime.fromtimestamp(time.time()).strftime(
        '%Y-%m-%d %H:%M:%S')
