from abc import ABC, abstractmethod

class StringableParameterInterface(ABC):

    @abstractmethod
    def toString(self) -> str:
        pass
