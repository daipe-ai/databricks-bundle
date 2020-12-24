from pathlib import Path

class NotebookPathResolverInterface:

    def resolve(self) -> Path:
        pass
