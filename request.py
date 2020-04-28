class Request:
    def __init__(self, operation, clock, id, message=None, broker=None):
        self.operation = operation
        self.clock = clock
        self.id = id
        self.broker = broker
        self.message = message

    def __str__(self) -> str:
        return f'Request(conn={self.broker}, operation={self.operation}, id={self.id}, clock={self.clock}, ' \
            f'message={self.message})'

    def __hash__(self):
        return hash(self.broker.broker_id)
