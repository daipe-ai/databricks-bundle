from datetime import datetime as dt


class WatcherLogger:
    LEVELS = {1: "debug", 2: "info", 3: "warning", 4: "error"}

    def __init__(self):
        self.messages = []

    def debug(self, message: str):
        self.messages.append((1, dt.now(), message))

    def info(self, message: str):
        self.messages.append((2, dt.now(), message))

    def warning(self, message: str):
        self.messages.append((3, dt.now(), message))

    def error(self, message: str):
        self.messages.append((4, dt.now(), message))

    def print(self, minimal_level=1):
        for message in self.messages:
            if message[0] >= minimal_level:
                print(WatcherLogger.LEVELS[message[0]] + ": " + message[1].strftime("%m.%d.%Y_%H:%M:%S") + " " + str(message[2]))
