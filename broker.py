import errno
import selectors
import socket
import struct
import time
from collections import deque

from connection_info import ConnectionInfo
from middleware import OperationType, SerializationType, ExternalBroker
from request import Request
from topic import Topic
from trie import Trie, Node, update_last_message
from utils import build_message, general_decode, general_encode, unpack_and_receive_message_bytes, \
    ConnectionClosedError, send_message_packed_with_size_to_another_broker, build_message_between_brokers, \
    send_to_all_brokers

# TODO: Verificar se todos os clocks estão no lugar (por exemplo, ajustamos o clock a seguir a cada send?)
class Broker:
    client_operations = (OperationType.SUBSCRIBE.value, OperationType.PUBLISH.value, OperationType.LIST.value,
                         OperationType.CANCEL.value)
    broker_operations = (OperationType.ALLOW.value, OperationType.ENTER.value,
                         OperationType.RELEASE.value, OperationType.GREETING.value)

    # TODO: port e brokers port podem ser enviados pela linha de comandos
    def __init__(self, number_listeners, port=8000, brokers_ports=None):
        self.port = port
        self.selector = selectors.DefaultSelector()
        self.broker_socket = socket.socket()
        self.number_listeners = number_listeners
        self.trie = Trie()
        self.connections = {}  # conn -> ConnectionInfo
        self.clock = 0
        self.request_queue = deque()
        self.serialization = SerializationType.JSON.value  # Default serialization
        self.brokers_dict = None  # conn
        self.broker_id = port
        self.brokers_ports = brokers_ports

    def connect_to_brokers(self):
        print('Waiting for brokers.')
        time.sleep(5)
        self.brokers_dict = {p: ExternalBroker(socket.create_connection(('localhost', p)), p) for p in
                             self.brokers_ports}

        send_to_all_brokers(self.brokers_dict.values(), str.encode(self.serialization))

        message = build_message_between_brokers(OperationType.GREETING.value, self.clock, "", self.broker_id)
        message_bytes = general_encode(self.serialization, message)
        send_to_all_brokers(self.brokers_dict.values(), message_bytes)

    def run(self):
        self.broker_socket.bind(('0.0.0.0', self.port))
        self.broker_socket.listen(self.number_listeners)
        self.broker_socket.setblocking(False)

        self.selector.register(self.broker_socket, selectors.EVENT_READ, self.accept)
        self.connect_to_brokers()

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
            if message["operation"] in self.client_operations:
                self.handle_client_message(conn, message)
            elif message["operation"] in self.broker_operations:
                self.handle_broker_message(message)
            else:
                print(f"Unknown operation type: {message['operation']}")
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

    def handle_client_message(self, conn, message):
        topic_path = message["topic"]
        operation = message["operation"]
        if operation not in self.client_operations:
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

    def handle_broker_message(self, message):
        operation = message["operation"]
        broker_id = message["broker_id"]
        broker = self.brokers_dict[broker_id]

        if operation == OperationType.ALLOW.value:
            self.handle_allow(broker, message)
        elif operation == OperationType.ENTER.value:
            self.handle_enter(broker, message)
        elif operation == OperationType.RELEASE.value:
            self.handle_release(broker, message)
        elif operation == OperationType.GREETING.value:
            self.handle_greeting(broker, message)

    def handle_publish(self, topic_node, message):
        self.request_to_enter(topic_node, message)

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

    def handle_allow(self, broker, message):
        requester_clock = message["clock"]
        self.clock_adjustments(requester_clock)

        request = Request(OperationType.ALLOW.value, requester_clock, message["id"], broker=broker)
        self.request_queue.append(request)

        self.sort_queue()
        if self.allowed_to_release():
            self.release()

    def handle_enter(self, broker, message):
        requester_clock = message["clock"]
        self.clock_adjustments(requester_clock)

        request = Request(OperationType.ENTER.value, requester_clock, message["id"], message["content"], broker)
        self.request_queue.append(request)

        self.allow_to_enter(broker.outgoing_conn, message["id"])
        self.sort_queue()

    def handle_release(self, broker, message):
        requester_clock = message["clock"]
        self.clock_adjustments(requester_clock)

        # TODO: Confirma se a mensagem que sai aqui é a correta
        released_request = self.request_queue[0]
        message_to_be_published = released_request["content"]

        self.publish(message_to_be_published)
        self.clean_up_queue(released_request.id)

    @staticmethod
    def handle_greeting(broker, message):
        broker.greetings_ack = True
        print(f"Received greetings from broker {broker.broker_id}: ")
        print(message)

    def clock_adjustments(self, requester_clock=0):
        self.clock = max(self.clock, requester_clock)
        self.clock = self.clock + 1

    def sort_queue(self):
        sorted(self.request_queue, key=lambda request: request.clock)

    def clean_up_queue(self, message_id):
        for request in self.request_queue:
            if request.id == message_id:
                self.request_queue.remove(request)

    def request_to_enter(self, topic_node, message):
        self.clock_adjustments()

        timestamp = message["timestamp"]
        content = message["content"]
        serialization = message["serialization"]
        message_to_publish = build_message(serialization, OperationType.PUBLISH.value, topic_node.topic.path, content,
                                           timestamp)

        request_message = build_message_between_brokers(OperationType.ENTER.value, self.clock, message_to_publish, self.broker_id)
        request = Request(OperationType.ENTER.value, self.clock, request_message["id"], None, message_to_publish)

        self.request_queue.append(request)
        self.sort_queue()
        send_to_all_brokers(self.brokers_dict.values(), request_message)

    def allow_to_enter(self, broker, message_id):
        self.clock = self.clock + 1
        message = build_message_between_brokers(OperationType.ALLOW.value, self.clock, "", broker.broker_id, message_id)
        message_bytes = general_encode(self.serialization, message)
        send_message_packed_with_size_to_another_broker(broker, message_bytes)

    def release(self):
        self.clock = self.clock + 1
        first_request = self.request_queue[0]

        message = build_message_between_brokers(OperationType.RELEASE.value, self.clock, "", self.broker_id,
                                                first_request.id)
        message_bytes = general_encode(self.serialization, message)
        send_to_all_brokers(self.brokers_dict.values(), message_bytes)

        self.clean_up_queue(first_request.id)

    def allowed_to_release(self):
        if self.request_queue:  # Check if it is not empty
            first_request = self.request_queue[0]

            # Check if the first request corresponds to our broker
            if first_request.broker == self.broker_id:
                return False
            first_request_id = first_request.id
            counter_allows_for_id = 0

            # Check if the broker has received as many allow responses for a specific id
            for request in self.request_queue:
                if request.operation == OperationType.ALLOW.value and request.id == first_request_id:
                    counter_allows_for_id += 1

            return len(self.brokers_dict) == len(counter_allows_for_id)
        return False

    def publish(self, message):
        topic_path = message["topic"]
        topic_node = self.trie.find_node(topic_path)
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
    message_broker = Broker(50)
    print("Broker is running...")
    message_broker.run()
