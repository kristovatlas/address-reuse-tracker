"""A module for making HTTP queries."""

import urllib2          # web scraping
from time import sleep  # pausing before retrying when API site is down
import ssl              # ssl.SSLError

import logger

MAX_RETRY_TIME_IN_SEC = 60
#Note: If you are querying a large file, this timeout may cause that to fail.
#   You may need to adjust based on your bandwidth and the size of response
#   you're requesting.
NUM_SEC_TIMEOUT = 30

ENABLE_DEBUG_PRINT = True

def fetch_url(url):
    """Fetch contents of remote page as string for specified url."""

    current_retry_time_in_sec = 0

    dprint("Fetching url: %s" % url)

    response = ''
    while current_retry_time_in_sec <= MAX_RETRY_TIME_IN_SEC:
        if current_retry_time_in_sec:
            sleep(current_retry_time_in_sec)
        try:
            response = urllib2.urlopen(url=url, timeout=NUM_SEC_TIMEOUT).read()
            if response is None:
                #For some reason, no handler handled the request
                logger.log_and_die("No URL handler utilized.")
            return response
        except (urllib2.HTTPError, ssl.SSLError) as err:
            #There was a problem fetching the page, maybe something other than
            #   HTTP 200 OK.
            if current_retry_time_in_sec == MAX_RETRY_TIME_IN_SEC:
                logger.log_and_die(("Could not fetch url '%s': Code is %s "
                                    "reason is '%s' full response: '%s'") %
                                   (url, err.code, err.reason, response))
            else:
                current_retry_time_in_sec = current_retry_time_in_sec + 1
                print(("Encountered HTTPError fetching '%s'. Will waiting for "
                       "%d seconds before retrying. Error was: '%s'") %
                      (url, current_retry_time_in_sec, str(err)))
        except urllib2.URLError as err:
            if current_retry_time_in_sec == MAX_RETRY_TIME_IN_SEC:
                logger.log_and_die(("Invalid URL '%s' could not be fetched: "
                                    "%s") % (url, str(err)))
            else:
                current_retry_time_in_sec = current_retry_time_in_sec + 1
                print(("Encountered URLError fetching '%s'. Will waiting for "
                       "%d seconds before retrying. Error was: '%s'") %
                      (url, current_retry_time_in_sec, str(err)))

def dprint(msg):
    """Print debug message."""
    if ENABLE_DEBUG_PRINT:
        print "DEBUG: %s" % msg
