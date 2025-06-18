from abc import ABC, abstractmethod

class NewsClient(ABC):
    def __init__(self, source_config):
        self.source = source_config

    @abstractmethod
    async def fetch(self, session, keyword, language, page_size):
        pass
