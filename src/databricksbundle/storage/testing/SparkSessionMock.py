class SparkConf:

    vals: dict = {}

    def set(self, key: str, val: str):
        self.vals[key] = val

    def get(self, key: str):
        if key not in self.vals:
            raise Exception(f"Conf value not found for {key}")

        return self.vals[key]


class SparkSessionMock:
    def __init__(self):
        self.conf = SparkConf()
