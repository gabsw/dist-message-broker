import sys
import argparse

import middleware


class Consumer:
    def __init__(self, datatype):
        self.type = datatype
        self.queue = middleware.PickleQueue(f"/{self.type}", type=middleware.MiddlewareType.CONSUMER)

    @classmethod
    def datatypes(self):
        return ["temp", "msg", "weather"]

    def run(self, length=10):
        while True:
            topic, data = self.queue.pull()
            print("topic--->{}, data--->{}".format(topic, data))
            if topic == "MIDDLEWARE":
                break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", help="type of producer: [temp, msg, weather]", default="weather")
    args = parser.parse_args()

    if args.type not in Consumer.datatypes():
        print("Error: not a valid producer type")
        sys.exit(1)

    p = Consumer(args.type)

    p.run()
