class Topic:
    def __init__(self, path):
        self.path = path
        self.last_message = None
        self.subscribers = set()  # set of ConnectionInfo

    def add_subscriber(self, connection_info):
        print("Adding subscriber", connection_info, "to", self)
        self.subscribers.add(connection_info)

    def remove_subscriber(self, connection_info):
        print("Removing subscriber", connection_info, "from", self)
        if connection_info in self.subscribers:
            self.subscribers.remove(connection_info)

    def __str__(self):
        return f"Topic(path={self.path})"

    def __repr__(self):
        return str(self)
