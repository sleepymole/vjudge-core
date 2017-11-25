import requests
import threading
import logging
from queue import Queue, Empty
from abc import ABCMeta, abstractclassmethod
from sqlalchemy import func
from datetime import datetime, timedelta
from config import OJ_ACCOUNTS, get_header
from .models import Submission, Problem
from . import db, exceptions

logging.basicConfig(level=logging.INFO)


class BaseClient(metaclass=ABCMeta):
    def __init__(self):
        self._session = requests.session()
        self._session.headers.update(get_header())

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


class VJudge(threading.Thread):
    def __init__(self, submit_queue, crawl_queue):
        super().__init__()
        self.submit_queue = submit_queue
        self.crawl_queue = crawl_queue
        self.judge_queues = {}
        self.status_queues = {}
        self.available_ojs = []

    def run(self):
        for oj_name in OJ_ACCOUNTS:
            self.judge_queues[oj_name] = Queue()
            self.status_queues[oj_name] = Queue()
            accounts = OJ_ACCOUNTS[oj_name]
            available = False
            for username in accounts:
                password = accounts[username]
                client = self._get_oj_client(oj_name, auth=(username, password))
                if client is not None:
                    logging.info("user '{}' log in to {} successfully".format(username, oj_name))
                    available = True
                    threading.Thread(target=self.judge, args=(client, oj_name, username), daemon=True).start()
            if available:
                self.available_ojs.append(oj_name)
                threading.Thread(target=self.refresh_status, args=(oj_name,), daemon=True).start()
        threading.Thread(target=self.handle_requests, daemon=True).start()
        threading.Thread(target=self.update_problem, daemon=True).start()
        if self.available_ojs:
            logging.info('{} are available'.format(self.available_ojs))
        else:
            logging.warning('there is no oj is available')
        self.refresh_status_all()
        self.update_problem_all()

    def judge(self, client, oj_name, user_id):
        queue = self.judge_queues[oj_name]
        while True:
            submission = Submission.query.get(queue.get())
            try:
                run_id = client.submit_problem(submission.problem_id, submission.language,
                                               submission.source_code)
            except (exceptions.SubmitError, exceptions.ConnectionError):
                submission.verdict = 'Submit Failed'
                db.session.commit()
            except exceptions.LoginExpired:
                try:
                    client.update_cookies()
                    queue.put(submission.id)
                except exceptions.ConnectionError:
                    submission.verdict = 'Submit Failed'
                    db.session.commit()
            else:
                submission.run_id = run_id
                submission.user_id = user_id
                submission.verdict = 'Being Judged'
                db.session.commit()
                self.status_queues[oj_name].put(submission.id)

    def refresh_status(self, oj_name):
        queue = self.status_queues[oj_name]
        client = self._get_oj_client(oj_name)
        while True:
            submission = Submission.query.get(queue.get(3600))
            if datetime.utcnow() - timedelta(hours=2) > submission.time_stamp:
                submission.verdict = 'Judge Timeout'
                db.session.commit()
                continue
            try:
                verdict, exe_time, exe_mem = client.get_submit_status(submission.run_id,
                                                                      user_id=submission.user_id,
                                                                      problem_id=submission.problem_id)
            except exceptions.ConnectionError:
                return
            if verdict in ('Being Judged', 'Queuing', 'Compiling', 'Running'):
                queue.put(submission.id)
            else:
                submission.verdict = verdict
                submission.exe_time = exe_time
                submission.exe_mem = exe_mem
                db.session.commit()

    def handle_requests(self):
        while True:
            id = self.submit_queue.get()
            submission = Submission.query.filter_by(id=id).one()
            if submission.oj_name not in self.available_ojs:
                submission.verdict = 'Submit Failed'
                db.session.commit()
                continue
            self.judge_queues[submission.oj_name].put(id)

    def refresh_status_all(self):
        submissions = Submission.query.filter(Submission.verdict == 'Being Judged').filter(
            Submission.run_id.isnot(None)).all()
        for submission in submissions:
            if submission.oj_name in self.available_ojs:
                self.status_queues[submission.oj_name].put(submission.id)

    def update_problem(self):
        clients = {}
        while True:
            try:
                oj_name, problem_id = self.crawl_queue.get(3600)
                if oj_name not in clients:
                    client = self._get_oj_client(oj_name)
                    if not client:
                        continue
                    clients[oj_name] = client
                client = clients[oj_name]
                try:
                    result = client.get_problem(problem_id)
                except exceptions.ConnectionError:
                    continue
                if not result:
                    continue
                p = Problem.query.filter_by(oj_name=oj_name, problem_id=problem_id).first() or Problem()
                for attr in result:
                    if hasattr(p, attr):
                        setattr(p, attr, result[attr])
                p.oj_name = oj_name
                p.problem_id = problem_id
                p.last_update = datetime.utcnow()
                db.session.add(p)
                db.session.commit()
                logging.info('problem update: {}'.format(p.summary()))
            except Empty:
                self.update_problem_all()

    def update_problem_all(self):
        outdated_problems = Problem.query.filter(
            datetime.utcnow() - timedelta(days=1) > Problem.last_update).all()
        for problem in outdated_problems:
            self.crawl_queue.put((problem.oj_name, problem.problem_id))
        result = db.session.query(
            Problem.oj_name.label('oj_name'),
            func.max(Problem.problem_id).label('max_id')).group_by(
            Problem.oj_name).all()
        for oj_name, max_id in result:
            for i in range(1, 21):
                problem_id = str(int(max_id) + i)
                self.crawl_queue.put((oj_name, problem_id))

    @staticmethod
    def _get_oj_client(oj_name, auth=None):
        import importlib
        try:
            oj = importlib.import_module('.' + oj_name, __package__)
        except ModuleNotFoundError:
            logging.error('oj {} is unavailable')
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
