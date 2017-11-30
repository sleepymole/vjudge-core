import logging
from abc import ABCMeta, abstractclassmethod

import requests

from config import get_header
from . import exceptions

logging.basicConfig(level=logging.INFO)


class BaseClient(metaclass=ABCMeta):
    def __init__(self):
        self._session = requests.session()
        self._session.headers.update(get_header())

    @abstractclassmethod
    def get_name(self):
        pass

    @abstractclassmethod
    def get_user_id(self):
        pass

    @abstractclassmethod
    def login(self, username, password):
        pass

    @abstractclassmethod
    def check_login(self):
        pass

    @abstractclassmethod
    def update_cookies(self):
        pass

    @abstractclassmethod
    def get_problem(self, problem_id):
        pass

    @abstractclassmethod
    def submit_problem(self, problem_id, language, source_code):
        pass

    @abstractclassmethod
    def get_submit_status(self, run_id, **kwargs):
        pass


def get_client(oj_name, auth=None):
    import importlib
    try:
        oj = importlib.import_module('.' + oj_name, __package__)
    except ModuleNotFoundError:
        logging.error('oj {} is not supported')
        return
    try:
        client = oj.Client()
        if auth is not None:
            client.login(*auth)
        return client
    except exceptions.LoginError:
        logging.error("user '{}' log in to {} failed: "
                      "no such user or wrong password".format(auth[0], oj_name))
    except exceptions.ConnectionError:
        logging.error("user '{}' log in to {} failed: "
                      "network is unreachable".format(auth[0], oj_name))
