from abc import ABC, abstractmethod

class OptionsFactoryInterface(ABC):

    @abstractmethod
    def create(self) -> dict:
        pass
