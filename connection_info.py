from utils import send_message_packed_with_size


class ConnectionInfo:
    def __init__(self, conn, addr, serialization=None):
        self.conn = conn
        self.addr = addr
        self.serialization = serialization
        self.subscribed_topics = set()

    def send(self, message_bytes):
        send_message_packed_with_size(self.conn, message_bytes)

    def add_subscribed_topic(self, topic):
        self.subscribed_topics.add(topic)

    def remove_subscribed_topic(self, topic):
        if topic in self.subscribed_topics:
            self.subscribed_topics.remove(topic)

    def __str__(self) -> str:
        return f'ConnectionInfo(addr={self.addr}, fd={self.conn.fileno()}, subscriptions={self.subscribed_topics}, ' \
            f'serialization={self.serialization})'

    def __hash__(self):
        return hash(self.conn)
