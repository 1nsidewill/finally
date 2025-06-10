from collections import OrderedDict

class odSet:
    def __init__(self):
        self.od = OrderedDict()
    def push(self, key, value):
        if key in self.od:
            del self.od[key]
        self.od[key] = value
    def pop(self):
        return self.od.popitem(last=False)
    
product_od = odSet()