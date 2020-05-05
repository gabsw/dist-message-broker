import sys
import argparse
from producer import Producer
from middleware import PORT

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", help="type of producer: [temp, msg, weather]", default="temp")
    parser.add_argument("--length", help="number of messages to be sent", default=10)
    args = parser.parse_args()

    if args.type not in Producer.datatypes():
        print("Error: not a valid producer type")
        sys.exit(1)

    p = Producer(args.type, PORT, "even")

    p.run(int(args.length))
