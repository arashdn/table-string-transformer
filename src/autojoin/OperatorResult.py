class OperatorResult:
    def __init__(self, gain, block):
        self.gain = gain
        self.block = block

    def __repr__(self):
        return f"Gain:{self.gain}, block = {str(self.block)}"

    def __lt__(self, other):
        return self.gain < other.gain
