from topic import Topic


class Trie:
    def __init__(self):
        self.children = {}  # path_part -> Node

    def add(self, topic):
        if topic.path == "/":
            path_parts = [""]
        else:
            path_parts = parse_path(topic.path)

        return add(self, path_parts, topic)

    def find_node(self, topic_path):
        if topic_path == "/":
            path_parts = [""]
        else:
            path_parts = parse_path(topic_path)

        if len(path_parts) == 0:
            return None

        first_part = path_parts[0]
        if first_part in self.children:
            return self.children[first_part].find_node(path_parts[1:])
        else:
            return None

    def find_all_nodes(self):
        nodes = []
        for path in self.children:
            nodes = nodes + self.children[path].find_all_nodes()
        return sorted(nodes, key=lambda topic: topic.path)

    def get_last_message(self, topic_path):
        return self.find_node(topic_path).topic.last_message

    def pretty_print(self):
        print("Trie:")
        for node in self.children.values():
            node.pretty_print()


class Node:
    def __init__(self, node_name, topic, parent):
        self.node_name = node_name
        self.topic = topic
        self.children = {}
        self.parent = parent

    def add(self, remaining_path_parts, topic, path_prefix=""):
        return add(self, remaining_path_parts, topic, path_prefix=path_prefix)

    def find_node(self, remaining_path_parts):
        if len(remaining_path_parts) == 0:
            return self

        else:
            first_part = remaining_path_parts[0]
            if first_part in self.children:
                return self.children[first_part].find_node(remaining_path_parts[1:])
            else:
                return None

    def find_all_nodes(self):
        nodes = [self.topic]
        for node in self.children:
            nodes = nodes + self.children[node].find_all_nodes()
        return nodes

    def find_ancestors_and_self(self):
        topics = [self.topic]  # Starts with self

        node = self
        while node.parent is not None:
            node = node.parent
            topics.append(node.topic)

        return topics

    def pretty_print(self, depth=0):
        indentation = "\t" * depth
        print(indentation, self)
        for node in self.children.values():
            node.pretty_print(depth + 1)

    def __str__(self):
        return f"Node(node_name={self.node_name}, topic_path={self.topic.path})"


# Auxiliary functions
def update_last_message(topics, last_message):
    for topic in topics:
        topic.last_message = last_message


def parse_path(topic_path):
    path_parts = topic_path.split("/")
    return path_parts


def add(trie_or_node, remaining_path_parts, topic, path_prefix=""):
    if len(remaining_path_parts) == 0:
        return trie_or_node

    first_part = remaining_path_parts[0]
    parent = None if path_prefix == "" else trie_or_node  # if the prefix is empty, then trie_or_node is the trie

    if first_part in trie_or_node.children:
        node = trie_or_node.children[first_part]
    elif len(remaining_path_parts) == 1:
        node = Node(first_part, topic, parent=parent)
        trie_or_node.children[first_part] = node
        return node
    else:
        new_topic_path = path_prefix + "/" + first_part
        new_topic = Topic(new_topic_path)
        node = Node(first_part, new_topic, parent=parent)
        trie_or_node.children[first_part] = node

    prefix = node.topic.path if node.topic.path != "/" else ""
    return node.add(remaining_path_parts[1:], topic, path_prefix=prefix)


if __name__ == '__main__':
    trie = Trie()
    topic1 = Topic("/A/B/C")
    topic2 = Topic("/A/B/D")
    topic3 = Topic("/C/B/D")
    topic4 = Topic("/A/B/C/D/E/F")
    topic5 = Topic("/C/B/D/H")
    trie.add(topic1)
    trie.add(topic2)
    trie.add(topic2)
    trie.add(topic3)
    trie.add(topic4)
    trie.add(topic5)
    print()
    print(trie.find_all_nodes())
    print(trie.find_node("/A/B"))
    print(trie.find_node("/A/E/F"))
    print()

    trie.pretty_print()
    print(trie.find_node("/A/B/C/D/E/F").find_ancestors_and_self())
