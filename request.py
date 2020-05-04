class Request:
    def __init__(self, operation, clock, message_id, broker_id, message=None):
        self.operation = operation
        self.clock = clock
        self.message_id = message_id
        self.broker_id = broker_id
        self.message = message

    def __str__(self) -> str:
        return f'Request(broker_id={self.broker_id}, operation={self.operation}, id={self.id}, clock={self.clock}, ' \
            f'message={self.message})'

    def __hash__(self):
        return hash(self.broker_id)
