import logging
from abc import abstractmethod, ABC

import requests

from config import get_header

logging.basicConfig(level=logging.INFO)


class BaseClient(ABC):
    def __init__(self):
        self._session = requests.session()
        self._session.headers.update(get_header())

    @abstractmethod
    def get_name(self):
        pass

    @abstractmethod
    def get_user_id(self):
        pass

    @abstractmethod
    def login(self, username, password):
        pass

    @abstractmethod
    def check_login(self):
        pass

    @abstractmethod
    def update_cookies(self):
        pass

    @abstractmethod
    def get_problem(self, problem_id):
        pass

    @abstractmethod
    def submit_problem(self, problem_id, language, source_code):
        pass

    @abstractmethod
    def get_submit_status(self, run_id, **kwargs):
        pass
