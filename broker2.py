from broker import Broker
from middleware import PORT


if __name__ == "__main__":
    broker = Broker(50, PORT + 1, [PORT])
    print("Broker is running...")
    broker.run()
