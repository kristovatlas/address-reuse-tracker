#Trying to make string concatenation syntax less ugly

####################
# INTERNAL IMPORTS #
####################

import validate

class StringBuilder:
    data = None
    
    def __init__(self, initial_val = None):
        if initial_val is not None:
            validate.check_str_and_die(initial_val, 'initial_val',
                                       'StringBuilder.__init__')
            self.data = str(initial_val)
        else:
            self.data = ''
    
    def append(self, new_val):
        validate.check_str_and_die(new_val, 'new_val', 'append')
        self.data = self.data + str(new_val)
    
    def __str__(self):
        return str(self.data)
