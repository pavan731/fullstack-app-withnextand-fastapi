from abc import ABC, abstractmethod
from backend.session_manager import SessionManager

class SessionFactory(ABC):
    @abstractmethod
    def create_session_manager(self) -> SessionManager:
        pass

class SessionManagerFactory(SessionFactory):
    def create_session_manager(self) -> SessionManager:
        return SessionManager()