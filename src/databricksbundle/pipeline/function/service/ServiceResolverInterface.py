from abc import ABC, abstractmethod
from pathlib import Path

class ServiceResolverInterface(ABC):

    @abstractmethod
    def resolve(self, pipelinePath: Path) -> object:
        pass
