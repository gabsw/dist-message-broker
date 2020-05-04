import argparse
import sys

from consumer import Consumer
from middleware import PORT

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", help="type of producer: [temp, msg, weather]", default="temp")
    args = parser.parse_args()

    if args.type not in Consumer.datatypes():
        print("Error: not a valid producer type")
        sys.exit(1)

    p = Consumer(args.type,PORT+1)

    p.run()
