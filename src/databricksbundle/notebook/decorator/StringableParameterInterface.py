from abc import ABC, abstractmethod


class StringableParameterInterface(ABC):
    @abstractmethod
    def to_string(self) -> str:
        pass
