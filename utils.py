import json
import datetime
import pickle
import xml.etree.ElementTree as xml
from enum import Enum
import struct


class SerializationType(Enum):
    JSON = "json"
    PICKLE = "pickle"
    XML = "xml"


def build_message(serialization, operation, topic, content, timestamp=None):
    return {"serialization": serialization,
            "operation": operation,
            "topic": topic,
            "timestamp": str(datetime.datetime.utcnow()) if timestamp is None else timestamp,
            "content": content}


def send_message_packed_with_size(socket, message_bytes):
    message_size = len(message_bytes)
    socket.sendall(struct.pack('!I', message_size))
    socket.sendall(message_bytes)


def unpack_and_receive_message_bytes(socket):
    buffer_size = socket.recv(4)  # We will always receive 4 bytes with the buffer size for the full message
    if not buffer_size:
        raise ConnectionClosedError
    length, = struct.unpack('!I', buffer_size)
    return receive_full_message_bytes(socket, length)


def receive_full_message_bytes(sock, bytes_counter):
    full_message = b''
    while bytes_counter:
        partial_message = sock.recv(bytes_counter)
        if not partial_message:
            return None
        full_message += partial_message
        bytes_counter -= len(partial_message)
    return full_message


def general_decode(serialization, message):
    if serialization == SerializationType.JSON.value:
        return decode_json(message)
    elif serialization == SerializationType.PICKLE.value:
        return decode_pickle(message)
    elif serialization == SerializationType.XML.value:
        return decode_xml(message)
    return None


def general_encode(serialization, message):
    if serialization == SerializationType.JSON.value:
        return encode_json(message)
    elif serialization == SerializationType.PICKLE.value:
        return encode_pickle(message)
    elif serialization == SerializationType.XML.value:
        return encode_xml(message)
    return None


def decode_json(message):
    try:
        return json.loads(message)
    except Exception as e:
        print("Decoding error ", message, e)
        return None


def encode_json(message):
    try:
        return json.dumps(message).encode('utf-8')
    except Exception as e:
        print("Encoding error ", message, e)
        return None


def decode_pickle(message):
    try:
        return pickle.loads(message)
    except Exception as e:
        print("Decoding error ", message, e)
        return None


def encode_pickle(message):
    try:
        return pickle.dumps(message)
    except Exception as e:
        print("Encoding error ", message, e)
        return None


def decode_xml(message):
    try:
        xml_str = message.decode()
        xml_msg = xml.fromstring(xml_str)
        serialization = xml_msg.find('serialization').text
        operation = xml_msg.find('operation').text
        topic = xml_msg.find('topic').text
        content = xml_msg.find('content').text
        timestamp = xml_msg.find('timestamp').text
        return build_message(serialization, operation, topic, content, timestamp)
    except Exception as e:
        print("Decoding error ", message, e)
        return None


def encode_xml(message):
    try:
        root = xml.Element("message")
        for key in message:
            new_xml(root, key, message[key])
        return xml.tostring(root, 'utf-8')
    except Exception as e:
        print("Encoding error ", message, e)
        return None


def new_xml(root, key, value):
    new_node = xml.SubElement(root, key)
    new_node.text = value.__str__()


class ConnectionClosedError(Exception):
    pass
