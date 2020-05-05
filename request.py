class Request:
    def __init__(self, operation, clock, message_id, broker_id, message=None):
        self.operation = operation
        self.clock = clock
        self.message_id = message_id
        self.broker_id = broker_id
        self.message = message

    def __str__(self):
        return f'Request(broker_id={self.broker_id}, operation={self.operation}, message_id={self.message_id},' \
            f' clock={self.clock}, data={self.message["content"] if self.message else None}'

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(self.broker_id)
