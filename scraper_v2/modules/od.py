from collections import OrderedDict

class odSet:
    def __init__(self):
        self.od = OrderedDict()
    def __len__(self):
        return len(self.od)
    def __sub__(self, other: "odSet"):
        result = odSet()
        for k, v in self.od.items():
            if k not in other.od or other.od[k] != v:
                result.push(k, v)
        return result
    def push(self, key, value):
        if key in self.od:
            del self.od[key]
        self.od[key] = value
    def pop(self):
        try:
            return self.od.popitem(last=False)
        except KeyError:
            return None
    def keyList(self):
        return list(self.od.keys())
    def valueList(self):
        return list(self.od.values())