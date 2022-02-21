
class RawPatternBlock:
    NAME = "PATTERN"

    def __init__(self, ):
        pass

    def apply(self, inp):
        raise NotImplementedError

    @classmethod
    def extract(cls, inp, blk):
        return set()

    def __eq__(self, other):
        raise NotImplementedError

    def __hash__(self):
        raise NotImplementedError

    def __repr__(self):
        return "[Unknown pattern representation]"
