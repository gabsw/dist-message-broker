import sys
import argparse
import middleware
import random
import time

text = ["Ó mar salgado, quanto do teu sal",
        "São lágrimas de Portugal!",
        "Por te cruzarmos, quantas mães choraram,",
        "Quantos filhos em vão rezaram!",
        "Quantas noivas ficaram por casar",
        "Para que fosses nosso, ó mar!",

        "Valeu a pena? Tudo vale a pena",
        "Se a alma não é pequena.",
        "Quem quer passar além do Bojador",
        "Tem que passar além da dor.",
        "Deus ao mar o perigo e o abismo deu,",
        "Mas nele é que espelhou o céu.", ]


class Producer:
    def __init__(self, datatype, port, valuetype=None):
        self.type = datatype
        self.valuetype = valuetype
        if datatype == "temp":
            self.queue = [middleware.JSONQueue(port, f"/{self.type}", middleware.MiddlewareType.PRODUCER)]
            self.gen = self._temp
        elif datatype == "msg":
            self.queue = [middleware.JSONQueue(port, f"/{self.type}", middleware.MiddlewareType.PRODUCER)]
            self.gen = self._msg
        elif datatype == "weather":
            self.queue = [middleware.JSONQueue(port, f"/{self.type}/temperature", middleware.MiddlewareType.PRODUCER),
                          middleware.JSONQueue(port, f"/{self.type}/humidity", middleware.MiddlewareType.PRODUCER),
                          middleware.JSONQueue(port, f"/{self.type}/pressure", middleware.MiddlewareType.PRODUCER)]
            self.gen = self._weather
        self.port = port

    @classmethod
    def datatypes(self):
        return ["temp", "msg", "weather"]

    def _temp(self):
        if self.valuetype is None:
            time.sleep(0.1)
            yield random.randint(0, 40)
        elif self.valuetype is "even":
            temp = random.randint(0, 40)
            while temp % 2 != 0:
                temp = random.randint(0, 40)
            yield temp
        elif self.valuetype is "odd":
            temp = random.randint(0, 40)
            while temp % 2 != 1:
                temp = random.randint(0, 40)
            yield temp

    def _msg(self):
        time.sleep(0.2)
        yield random.choice(text)

    def _weather(self):
        time.sleep(0.1)
        yield random.randint(0, 40)
        time.sleep(0.1)
        yield random.randint(0, 100)
        time.sleep(0.1)
        yield random.randint(10000, 11000)

    def run(self, length=10):
        for _ in range(length):
            for queue, value in zip(self.queue, self.gen()):
                queue.push(value)
                print(f'port {self.port} data {value}')
                time.sleep(0.1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", help="type of producer: [temp, msg, weather]", default="weather")
    parser.add_argument("--length", help="number of messages to be sent", default=10)
    args = parser.parse_args()

    if args.type not in Producer.datatypes():
        print("Error: not a valid producer type")
        sys.exit(1)

    p = Producer(args.type, "None")

    p.run(int(args.length))
