"""Generic functions for validating data formats."""

import sys #sys.maxint
import re

import address_reuse.logger
logger = address_reuse.logger

#https://docs.python.org/2/library/stdtypes.html#numeric-types-int-float-long-complex
MININT = -sys.maxint - 1

def check_int(the_int):
    """Check integer for troublesome values & throw error if bad."""
    try:
        int(the_int)
    except ValueError:
        raise ValueError('Variable in check_int() is not an integer')
    if the_int == sys.maxint:
        raise ValueError('Integer equals the system maximum value')
    elif the_int == MININT:
        raise ValueError('Integer equals the system minimum value')

def check_float(the_float):
    """Check float for troublesome values & throw error if bad."""
    try:
        float(the_float)
    except ValueError:
        raise ValueError('Variable in check_float() is not a float')
    if the_float == sys.float_info.max:
        raise ValueError('Float equals the system maximum value')
    elif the_float == sys.float_info.min:
        raise ValueError('Float equals the system minimum value')

def check_str(the_str):
    """Check string for troublesome values & throw error if bad."""
    try:
        str(the_str)
    except ValueError:
        raise ValueError('Variable in check_str() cannot be cast to a string')

def check_int_and_die(the_int, var_name, caller_name):
    """Check int for troublesome values & stop process if bad after logging."""
    try:
        check_int(the_int)
    except ValueError as err:
        address_reuse.logger.log_and_die('Exceptional value for %s in %s: %s' %
                                         (var_name, caller_name, str(err)))

def check_float_and_die(the_float, var_name, caller_name):
    """Check float for troublesome values. If bad, log and stop process."""
    try:
        check_float(the_float)
    except ValueError as err:
        logger.log_and_die('Exceptional value for %s in %s: %s' %
                           (var_name, caller_name, str(err)))

def check_str_and_die(the_str, var_name, caller_name):
    """Check string for troublesome values. If bad, log and stop process."""
    try:
        check_str(the_str)
    except ValueError as err:
        logger.log_and_die("Cannot cast value '%s' to a string in %s: %s" %
                           (var_name, caller_name, str(err)))

def looks_like_address(the_str):
    """Checks whether string is formatted as plausible Bitcoin address."""
    return _is_match(r"^1|3\w{25,34}$", the_str)

def looks_like_hex(the_str):
    """Checks whether string is formatted as plausible hex string."""
    return _is_match("^[0123456789abcdefABCEDF]+$", the_str)

def check_hex_and_die(hex_str, caller_name):
    """If string isn't a plausible hex string, write log and stop process."""
    if not looks_like_hex(hex_str):
        logger.log_and_die("Exceptional value '%s' for hex string in %s" %
                           (str(hex_str), caller_name))

def check_address_and_die(btc_address, caller_name):
    """If string isn't a plausible Bitcoin address, write log & stop process."""
    if not looks_like_address(btc_address):
        logger.log_and_die("Exceptional value '%s' for address in %s" %
                           (btc_address, caller_name))

def _get_matches(regex, string):
    """Get regex match() results."""
    result = re.match(regex, string)
    return result

def _is_match(regex, string):
    """Determines whether regex matches string."""
    matches = _get_matches(regex, string)
    return matches is not None and matches.group() is not None
