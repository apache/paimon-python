class SequenceGenerator:

    def __init__(self, start: int = 0):
        self.current = start
    
    def next(self) -> int:
        self.current += 1
        return self.current
    
    def current_value(self) -> int:
        return self.current
    
    def reset(self, value: int = 0):
        self.current = value
