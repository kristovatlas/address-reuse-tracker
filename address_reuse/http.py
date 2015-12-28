####################
# INTERNAL IMPORTS #
####################

import logger

####################
# EXTERNAL IMPORTS #
####################

import urllib2          # web scraping
from time import sleep  # pausing before retrying when API site is down
import ssl              # ssl.SSLError

#############
# CONSTANTS #
#############

MAX_RETRY_TIME_IN_SEC = 60
#Note: If you are querying a large file, this timeout may cause that to fail.
#   You may need to adjust based on your bandwidth and the size of response 
#   you're requesting.
NUM_SEC_TIMEOUT = 30

#############
# FUNCTIONS #
#############
def fetch_url(url):
    current_retry_time_in_sec = 0
    
    print("DEBUG: Fetching url: %s" % url)
    
    response = ''
    while current_retry_time_in_sec <= MAX_RETRY_TIME_IN_SEC:
        if current_retry_time_in_sec:
            sleep(current_retry_time_in_sec)
        try:
            #print("DEBUG: About to open...")
            response = urllib2.urlopen(url=url, timeout=NUM_SEC_TIMEOUT).read()
            #print("DEBUG: Received response of length %d" % len(response))
            if response is None:
                #For some reason, no handler handled the request
                logger.log_and_die("No URL handler utilized.")
            return response
        except urllib2.URLError as err:
            if current_retry_time_in_sec == MAX_RETRY_TIME_IN_SEC:
                logger.log_and_die(("Invalid URL '%s' could not be fetched: "
                                    "%s") % (url, str(err)))
            else:
                current_retry_time_in_sec = current_retry_time_in_sec + 1
                print(("Encountered URLError fetching '%s'. Will waiting for "
                       "%d seconds before retrying. Error was: '%s'") % 
                      (url, current_retry_time_in_sec, str(err)))
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
