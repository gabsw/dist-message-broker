import struct
from enum import Enum
import socket
from utils import build_message, general_encode, general_decode, SerializationType, send_message_packed_with_size, \
    unpack_and_receive_message_bytes, ConnectionClosedError

HOST = 'localhost'
PORT = 8000


class MiddlewareType(Enum):
    CONSUMER = 1
    PRODUCER = 2
    BROKER = 3


class OperationType(Enum):
    PUBLISH = "publish"
    SUBSCRIBE = "subscribe"
    LIST = "list"
    CANCEL = "cancel"
    GREETING = "greeting"
    ALLOW = "allow"
    ENTER = "enter"
    RELEASE = "release"


class Queue:
    def __init__(self, port,serialization_type, topic, type=MiddlewareType.CONSUMER):
        self.topic = topic
        self.queue_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.queue_socket.connect((HOST, port))
        self.subscribed = False
        self.type = type
        self.serialization_type = serialization_type
        self.send_serialization()
        self.connection_closed = False

    def push(self, value):
        message = build_message(self.serialization_type, OperationType.PUBLISH.value, self.topic, value)
        message_bytes = general_encode(self.serialization_type, message)
        send_message_packed_with_size(self.queue_socket, message_bytes)

    def pull(self):
        """ Receives (topic, data) from broker.

            Should block the consumer!"""
        if self.connection_closed:
            return "MIDDLEWARE", "Connection was closed by the host."

        if not self.subscribed:
            self.subscribe()

        try:
            message_bytes = unpack_and_receive_message_bytes(self.queue_socket)
        except ConnectionClosedError:
            self.queue_socket.close()
            self.connection_closed = True
            return "MIDDLEWARE", "Connection was closed by the host."
        # In the case that unpacking the message from the broker fails
        except struct.error as e:
            return None, None

        decoded_message = general_decode(self.serialization_type, message_bytes)
        return decoded_message["topic"], decoded_message["content"]

    def send_serialization(self):
        serialization = str.encode(self.serialization_type)
        send_message_packed_with_size(self.queue_socket, serialization)

    def subscribe(self):
        message = build_message(self.serialization_type, OperationType.SUBSCRIBE.value, self.topic, "")
        message_bytes = general_encode(self.serialization_type, message)
        send_message_packed_with_size(self.queue_socket, message_bytes)
        self.subscribed = True

    def list(self):
        message = build_message(self.serialization_type, OperationType.LIST.value, None, "")
        message_bytes = general_encode(self.serialization_type, message)
        send_message_packed_with_size(self.queue_socket, message_bytes)

    def cancel(self):
        message = build_message(self.serialization_type, OperationType.CANCEL.value, self.topic, "")
        message_bytes = general_encode(self.serialization_type, message)
        send_message_packed_with_size(self.queue_socket, message_bytes)
        self.subscribed = False


class JSONQueue(Queue):
    def __init__(self, port,topic, type=MiddlewareType.CONSUMER):
        super().__init__(port,SerializationType.JSON.value, topic, type)

    def push(self, value):
        """ Sends data to broker. """
        super().push(value)

    def pull(self):
        """ Receives (topic, data) from broker.

            Should block the consumer!"""
        return super().pull()

    def subscribe(self):
        super().subscribe()

    def list(self):
        super().list()

    def cancel(self):
        super().cancel()

    def __str__(self):
        return f'JSONQueue({self.topic})'


class XMLQueue(Queue):
    def __init__(self,port, topic, type=MiddlewareType.CONSUMER):
        super().__init__(port, SerializationType.XML.value, topic, type)

    def push(self, value):
        """ Sends data to broker. """
        super().push(value)

    def pull(self):
        """ Receives (topic, data) from broker.

            Should block the consumer!"""
        return super().pull()

    def subscribe(self):
        super().subscribe()

    def list(self):
        super().list()

    def cancel(self):
        super().cancel()

    def __str__(self):
        return f'XMLQueue({self.topic})'


class PickleQueue(Queue):
    def __init__(self,port, topic, type=MiddlewareType.CONSUMER):
        super().__init__(port,SerializationType.PICKLE.value, topic, type)

    def push(self, value):
        """ Sends data to broker. """
        super().push(value)

    def pull(self):
        """ Receives (topic, data) from broker.

            Should block the consumer!"""
        return super().pull()

    def subscribe(self):
        super().subscribe()

    def list(self):
        super().list()

    def cancel(self):
        super().cancel()

    def __str__(self):
        return f'PickleQueue({self.topic})'


class ExternalBroker:
    def __init__(self, outgoing_conn, id):
        self.outgoing_conn = outgoing_conn
        self.id = id
        self.greetings_ack = False
