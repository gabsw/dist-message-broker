import selectors
import socket
import errno
import struct

from connection_info import ConnectionInfo
from middleware import OperationType, SerializationType, PORT
from topic import Topic
from trie import Trie, Node, update_last_message
from utils import build_message, general_decode, general_encode, unpack_and_receive_message_bytes, ConnectionClosedError


class Broker:
    def __init__(self, number_listeners):
        self.selector = selectors.DefaultSelector()
        self.broker_socket = socket.socket()
        self.number_listeners = number_listeners
        self.trie = Trie()
        self.connections = {}  # conn -> ConnectionInfo

    def run(self):
        self.broker_socket.bind(('', PORT))
        self.broker_socket.listen(self.number_listeners)
        self.broker_socket.setblocking(False)

        self.selector.register(self.broker_socket, selectors.EVENT_READ, self.accept)

        while True:
            events = self.selector.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)

    def accept(self, sock, mask):
        conn, addr = self.broker_socket.accept()

        if conn not in self.connections:
            connection_info = ConnectionInfo(conn, addr)
            self.connections[conn] = connection_info
            print('Accepted', connection_info)
            conn.setblocking(False)
            self.selector.register(conn, selectors.EVENT_READ, self.read)

        else:
            conn.close()

    def read(self, conn, mask):
        connection_info = self.connections[conn]

        try:
            message_bytes = unpack_and_receive_message_bytes(conn)
        except ConnectionResetError:
            return
        except ConnectionClosedError:
            print('closing', conn)
            print("Client unregistered")
            self.remove_closed_connection(conn)
            return
        except BlockingIOError as e:
            # Sometimes we are receiving sockets that would have blocked on recv
            # this is a workaround
            if e.errno == errno.EWOULDBLOCK:
                return
            else:
                print('closing', conn)
                print("Client unregistered")
                self.remove_closed_connection(conn)
                return
        except struct.error as e:
            print('closing', conn)
            print("Client unregistered")
            self.remove_closed_connection(conn)
            return

        if connection_info.serialization is None:
            self.handle_first_message(conn, message_bytes)
            return

        message = general_decode(connection_info.serialization, message_bytes)
        if message:
            print('Received message ', message, ' from ', self.connections[conn])
            self.handle_message(conn, message)
        else:
            print('closing', conn)
            print("Client unregistered")
            self.remove_closed_connection(conn)

    def get_topic_node(self, topic_path, operation):
        if topic_path is None:
            return None

        topic_node = self.trie.find_node(topic_path)
        if topic_node is None and operation in (OperationType.SUBSCRIBE.value, OperationType.PUBLISH.value):
            new_topic = Topic(topic_path)
            return self.trie.add(new_topic)

        return topic_node

    def handle_message(self, conn, message):
        topic_path = message["topic"]
        operation = message["operation"]
        if operation not in (OperationType.SUBSCRIBE.value, OperationType.PUBLISH.value, OperationType.LIST.value,
                             OperationType.CANCEL.value):
            # close and remove connection due to bad message
            self.remove_closed_connection(conn)
            return

        topic_node = self.get_topic_node(topic_path, operation)
        if operation == OperationType.PUBLISH.value:
            self.handle_publish(topic_node, message)
        elif operation == OperationType.SUBSCRIBE.value:
            self.handle_subscribe(conn, topic_node, message)
        elif operation == OperationType.LIST.value:
            self.handle_list(conn)
        elif operation == OperationType.CANCEL.value:
            self.handle_cancel(conn, topic_node)
        else:
            print(f"Unknown operation type: {message['operation']}")

    def handle_publish(self, topic_node, message):
        timestamp = message["timestamp"]
        content = message["content"]
        serialization = message["serialization"]
        self.publish(serialization, topic_node, content, timestamp)

    def handle_subscribe(self, conn, topic_node: Node, message):
        if topic_node is None:
            self.remove_closed_connection(conn)
            print(f"Unknown topic path: {message['topic']}")
            return
        connection_info = self.connections[conn]
        topic_node.topic.add_subscriber(connection_info)
        connection_info.add_subscribed_topic(topic_node.topic)

        last_message = topic_node.topic.last_message
        if last_message is not None:
            message_bytes = general_encode(connection_info.serialization, last_message)
            connection_info.send(message_bytes)

    def handle_list(self, conn):
        nodes = [topic.path for topic in self.trie.find_all_nodes()]
        connection_info = self.connections[conn]
        serialization = connection_info.serialization
        if len(nodes) == 0:
            nodes = "No topics yet"
        message = build_message(serialization, OperationType.LIST.value, None, nodes)
        message_bytes = general_encode(serialization, message)
        connection_info.send(message_bytes)

    def handle_cancel(self, conn, topic_node):
        if topic_node is not None:
            connection_info = self.connections[conn]
            topic_node.topic.remove_subscriber(connection_info)
            connection_info.remove_subscribed_topic(topic_node.topic)

    def handle_first_message(self, conn, message_bytes):
        serialization = message_bytes.decode()
        if serialization not in (SerializationType.JSON.value, SerializationType.PICKLE.value,
                                 SerializationType.XML.value):
            # close and remove connection due to bad message
            self.remove_closed_connection(conn)
            return

        self.connections[conn].serialization = serialization

    @staticmethod
    def publish(serialization, topic_node: Node, content, timestamp):
        message = build_message(serialization, OperationType.PUBLISH.value, topic_node.topic.path, content,
                                timestamp)

        relevant_topics = topic_node.find_ancestors_and_self()
        update_last_message(relevant_topics, message)

        message_cache = {}  # Key: SerializationType, Value: corresponding encoded message

        for topic in relevant_topics:
            for subscriber in topic.subscribers:  # each subscriber is a ConnectionInfo
                subscriber_serialization = subscriber.serialization

                # Check if the serialized message already exists in the cache
                if subscriber_serialization in message_cache:
                    subscriber.send(message_cache[subscriber_serialization])
                # If not, go through the encoding process
                else:
                    message_bytes = general_encode(subscriber_serialization, message)
                    message_cache[subscriber_serialization] = message_bytes
                    subscriber.send(message_bytes)

    def remove_closed_connection(self, conn):
        connection_info = self.connections[conn]
        topics = connection_info.subscribed_topics

        for topic in topics:
            topic.remove_subscriber(connection_info)

        self.connections.pop(conn)
        self.selector.unregister(conn)
        conn.close()


# Test broker

if __name__ == "__main__":
    broker = Broker(50)
    print("Broker is running...")
    broker.run()
