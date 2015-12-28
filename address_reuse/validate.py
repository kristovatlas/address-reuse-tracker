####################
# INTERNAL IMPORTS #
####################

import logger

####################
# EXTERNAL IMPORTS #
####################

import sys          #sys.maxint
import re

#############
# CONSTANTS #
#############

#https://docs.python.org/2/library/stdtypes.html#numeric-types-int-float-long-complex
MININT = -sys.maxint - 1

#check integer for troublesome values and throw error if there's a problemo
def check_int(the_int):
    try:
        int(the_int)
    except ValueError:
        raise ValueError('Variable in check_int() is not an integer')
    if the_int == sys.maxint:
        raise ValueError('Integer equals the system maximum value')
    elif the_int == MININT:
        raise ValueError('Integer equals the system minimum value')

def check_float(the_float):
    try:
        float(the_float)
    except ValueError:
        raise ValueError('Variable in check_float() is not a float')
        if the_float == sys.float_info.max:
            raise ValueError('Float equals the system maximum value')
        elif the_float == sys.float_info.min:
            raise ValueError('Float equals the system minimum value')

def check_str(the_str):
    try:
        str(the_str)
    except ValueError:
        raise ValueError('Variable in check_str() cannot be cast to a string')

def check_int_and_die(the_int, var_name, caller_name):
    try:
        check_int(the_int)
    except ValueError as e:
        logger.log_and_die('Exceptional value for %s in %s: %s' % (var_name, caller_name, str(e)))
        
def check_float_and_die(the_float, var_name, caller_name):
    try:
        check_float(the_float)
    except ValueError as e:
        logger.log_and_die('Exceptional value for %s in %s: %s' % (var_name, caller_name, str(e)))

def check_str_and_die(the_str, var_name, caller_name):
    try:
        check_str(the_str)
    except valueError as e:
        logger.log_and_die("Cannot cast value '%s' to a string in %s: %s" (var_name, caller_name, str(e)))

def looks_like_address(str):
	return is_match("^1|3\w{25,34}$", str)

def looks_like_hex(str):
    return is_match("^[0123456789abcdefABCEDF]+$", str)

def check_hex_and_die(hex_str, caller_name):
    if not looks_like_hex(hex_str):
        logger.log_and_die("Exceptional value '%s' for hex string in %s" % (str(hex_str), caller_name))

def check_address_and_die(btc_address, caller_name):
    if not looks_like_address(btc_address): 
        logger.log_and_die("Exceptional value '%s' for address in %s" % (btc_address, caller_name))

def get_matches(regex, string):
	p = re.compile(regex)
	m = p.match(string)
	return m

def is_match(regex, string):
	matches = get_matches(regex, string)
	if matches is not None and matches.group() is not None:
		return True
	else:
		return False
