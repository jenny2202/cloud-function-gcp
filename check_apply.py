class check:
    def __init__(self,x):
        self.x = x
    
    def entity(self):
        if self.x['is_entity'] == 0:
            return 'FP - Full name not a match'
        else:
            return None
    
    def DOB(self):
        if self.x['is_DOB'] == 0:
            return 'FP - DOB not a match'
        else:
            return None
    
    def address(self):
        if self.x['is_address'] == 0:
            return 'FP - Address not a match'
        else:
            return None