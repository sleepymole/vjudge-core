import json
import logging
import threading
from abc import ABCMeta, abstractclassmethod
from datetime import datetime, timedelta

import redis
import requests
from sqlalchemy import func

from config import OJ_CONFIG, REDIS_CONFIG, get_header
from . import db, exceptions
from .models import Submission, Problem

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


def get_submit_queue(oj_name):
    return '-'.join((REDIS_CONFIG['queue']['submit_queue'], oj_name))


def get_status_queue(oj_name):
    return '-'.join((REDIS_CONFIG['queue']['status_queue'], oj_name))


def get_problem_queue(oj_name):
    return '-'.join((REDIS_CONFIG['queue']['problem_queue'], oj_name))


class Submitter(threading.Thread):
    def __init__(self, client, oj_name, user_id, pool=None):
        super().__init__(daemon=True)
        self.client = client
        self.oj_name = oj_name
        self.user_id = user_id
        self.pool = pool

    def run(self):
        r = redis.StrictRedis(connection_pool=self.pool)
        submit_queue = get_submit_queue(self.oj_name)
        status_queue = get_status_queue(self.oj_name)
        while True:
            submission = Submission.query.get(int(r.blpop(submit_queue)[1]))
            try:
                run_id = self.client.submit_problem(submission.problem_id, submission.language,
                                                    submission.source_code)
            except (exceptions.SubmitError, exceptions.ConnectionError):
                submission.verdict = 'Submit Failed'
                db.session.commit()
            except exceptions.LoginExpired:
                try:
                    self.client.update_cookies()
                    r.rpush(submit_queue, submission.id)
                except exceptions.ConnectionError:
                    submission.verdict = 'Submit Failed'
                    db.session.commit()
            else:
                submission.run_id = run_id
                submission.user_id = self.user_id
                submission.verdict = 'Being Judged'
                db.session.commit()
                r.rpush(status_queue, submission.id)


class StatusCrawler(threading.Thread):
    def __init__(self, oj_name, pool=None):
        super().__init__(daemon=True)
        self.oj_name = oj_name
        self.pool = pool

    def run(self):
        client = get_client(self.oj_name)
        if not client:
            return
        r = redis.StrictRedis(connection_pool=self.pool)
        status_queue = get_status_queue(self.oj_name)
        while True:
            submission = Submission.query.get(int(r.blpop(status_queue)[1]))
            if datetime.utcnow() - submission.time_stamp > timedelta(hours=2):
                submission.verdict = 'Judge Timeout'
                db.session.commit()
                continue
            try:
                verdict, exe_time, exe_mem = client.get_submit_status(
                    submission.run_id,
                    user_id=submission.user_id,
                    problem_id=submission.problem_id)
            except exceptions.ConnectionError:
                submission.verdict = 'Judge Timeout'
                db.session.commit()
                continue
            if verdict in ('Being Judged', 'Queuing', 'Compiling', 'Running'):
                r.rpush(status_queue, submission.id)
            else:
                submission.verdict = verdict
                submission.exe_time = exe_time
                submission.exe_mem = exe_mem
                db.session.commit()


class ProblemCrawler(threading.Thread):
    def __init__(self, oj_name, pool=None):
        super().__init__(daemon=True)
        self.oj_name = oj_name
        self.pool = pool

    def run(self):
        client = get_client(self.oj_name)
        if not client:
            return
        r = redis.StrictRedis(connection_pool=self.pool)
        problem_queue = get_problem_queue(self.oj_name)
        while True:
            problem_id = r.blpop(problem_queue)[1].decode()
            try:
                result = client.get_problem(problem_id)
            except exceptions.ConnectionError:
                continue
            if not result:
                continue
            problem = Problem.query.filter_by(
                oj_name=self.oj_name, problem_id=problem_id).first() or Problem()
            for attr in result:
                if hasattr(problem, attr):
                    setattr(problem, attr, result[attr])
            problem.oj_name = self.oj_name
            problem.problem_id = problem_id
            problem.last_update = datetime.utcnow()
            db.session.add(problem)
            db.session.commit()
            logging.info('problem update: {}'.format(problem.summary()))


class VJudge(object):
    def __init__(self):
        with open(OJ_CONFIG) as f:
            oj_config = json.load(f)
        self.accounts = oj_config['accounts']
        self.pool = redis.ConnectionPool(host=REDIS_CONFIG['host'], port=REDIS_CONFIG['port'])
        self.redis_con = redis.StrictRedis(connection_pool=self.pool)
        self.submit_queue = REDIS_CONFIG['queue']['submit_queue']
        self.problem_queue = REDIS_CONFIG['queue']['problem_queue']
        self.status_queue = REDIS_CONFIG['queue']['status_queue']
        self.available_ojs = []

    def start(self):
        for oj_name in self.accounts:
            self._add_judge(oj_name)
        if not self.available_ojs:
            logging.warning('there is no oj is available')
        else:
            logging.info('{} are available'.format(self.available_ojs))
        threading.Thread(target=self._handle_problem_queue, daemon=True).start()
        self.refresh_status()
        self.refresh_problem()
        last = datetime.utcnow() - timedelta(minutes=30)
        queue = self.submit_queue
        while True:
            submission = Submission.query.get(int(self.redis_con.blpop(queue)[1]))
            if submission.oj_name in self.available_ojs:
                self.redis_con.rpush(get_submit_queue(submission.oj_name), submission.id)
            else:
                if datetime.utcnow() - last > timedelta(minutes=30) \
                        and submission.oj_name in self.accounts:
                    self._add_judge(submission.oj_name)
                    self.redis_con.rpush(queue, submission.id)
                else:
                    submission.verdict = 'Submit Failed'
                    db.session.commit()

    def refresh_status(self):
        submissions = Submission.query.filter(Submission.verdict == 'Being Judged').filter(
            Submission.run_id.isnot(None)).all()
        for submission in submissions:
            if submission.oj_name in self.available_ojs:
                self.redis_con.rpush(get_status_queue(submission.oj_name), submission.id)

    def refresh_problem(self):
        queue = self.problem_queue
        outdated_problems = Problem.query.filter(
            datetime.utcnow() - timedelta(days=1) > Problem.last_update).all()
        for problem in outdated_problems:

            self.redis_con.rpush(queue, json.dumps(
                {'oj_name': problem.oj_name, 'problem_id': problem.problem_id}))
        result = db.session.query(
            Problem.oj_name.label('oj_name'),
            func.max(Problem.problem_id).label('max_id')).group_by(
            Problem.oj_name).all()
        for oj_name, max_id in result:
            for i in range(1, 21):
                problem_id = str(int(max_id) + i)
                self.redis_con.rpush(queue, json.dumps(
                    {'oj_name': oj_name, 'problem_id': problem_id}))

    def _handle_problem_queue(self):
        queue = self.problem_queue
        while True:
            result = self.redis_con.blpop(queue, timeout=3600)
            if result:
                try:
                    data = json.loads(result[1])
                    oj_name = data['oj_name']
                    problem_id = data['problem_id']
                except (json.decoder.JSONDecodeError, ValueError):
                    continue
                if oj_name in self.available_ojs:
                    self.redis_con.rpush(get_problem_queue(oj_name), problem_id)
            else:
                self.refresh_problem()

    def _add_judge(self, oj_name):
        if oj_name not in self.accounts:
            return False
        available = False
        for username in self.accounts[oj_name]:
            password = self.accounts[oj_name][username]
            client = get_client(oj_name, auth=(username, password))
            if client is not None:
                logging.info("user '{}' log in to {} successfully".format(username, oj_name))
                available = True
                Submitter(client, oj_name, username, pool=self.pool).start()
        if available:
            self.available_ojs.append(oj_name)
            StatusCrawler(oj_name, pool=self.pool).start()
            ProblemCrawler(oj_name, pool=self.pool).start()
        return available
