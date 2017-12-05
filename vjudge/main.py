import json
import logging
import threading
import time
from datetime import datetime, timedelta
from queue import Queue

import redis
from sqlalchemy import func

from config import REDIS_CONFIG, OJ_CONFIG
from .base import get_client, exceptions
from .models import db, Submission, Problem

logging.basicConfig(level=logging.INFO)


class Submitter(threading.Thread):
    def __init__(self, client, submit_queue, status_queue, daemon=False):
        super().__init__(daemon=daemon)
        self.client = client
        self.oj_name = client.get_name()
        self.user_id = client.get_user_id()
        self.submit_queue = submit_queue
        self.status_queue = status_queue

    def run(self):
        while True:
            submission = Submission.query.get(self.submit_queue.get())
            try:
                run_id = self.client.submit_problem(submission.problem_id, submission.language,
                                                    submission.source_code)
            except (exceptions.SubmitError, exceptions.ConnectionError):
                submission.verdict = 'Submit Failed'
                db.session.commit()
                logging.info('problem submit: {}'.format(submission.to_json()))
            except exceptions.LoginExpired:
                try:
                    self.client.update_cookies()
                    self.submit_queue.put(submission.id)
                except exceptions.ConnectionError:
                    submission.verdict = 'Submit Failed'
                    db.session.commit()
                    logging.info('problem submit: {}'.format(submission.to_json()))
            else:
                submission.run_id = run_id
                submission.user_id = self.user_id
                submission.verdict = 'Being Judged'
                db.session.commit()
                logging.info('problem submit: {}'.format(submission.to_json()))
                self.status_queue.put(submission.id)


class StatusCrawler(threading.Thread):
    def __init__(self, client, status_queue, daemon=None):
        super().__init__(daemon=daemon)
        self.client = client
        self.oj_name = client.get_name()
        self.status_queue = status_queue

    def run(self):
        while True:
            submission = Submission.query.get(self.status_queue.get())
            if datetime.utcnow() - submission.time_stamp > timedelta(hours=1):
                submission.verdict = 'Judge Failed'
                db.session.commit()
                continue
            try:
                verdict, exe_time, exe_mem = self.client.get_submit_status(
                    submission.run_id,
                    user_id=submission.user_id,
                    problem_id=submission.problem_id)
            except exceptions.ConnectionError:
                submission.verdict = 'Judge Failed'
                db.session.commit()
                continue
            if verdict in ('Being Judged', 'Queuing', 'Compiling', 'Running'):
                self.status_queue.put(submission.id)
            else:
                submission.verdict = verdict
                submission.exe_time = exe_time
                submission.exe_mem = exe_mem
                db.session.commit()


class ProblemCrawler(threading.Thread):
    def __init__(self, client, problem_queue, daemon=None):
        super().__init__(daemon=daemon)
        self.client = client
        self.oj_name = client.get_name()
        self.problem_queue = problem_queue

    def run(self):
        while True:
            problem_id = self.problem_queue.get()
            try:
                result = self.client.get_problem(problem_id)
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


class SubmitQueueHandler(threading.Thread):
    def __init__(self, queues, pool=None, daemon=None):
        super().__init__(daemon=daemon)
        self.redis_key = REDIS_CONFIG['queue']['submit_queue']
        self.redis_con = redis.StrictRedis(connection_pool=pool)
        self.queues = queues

    def run(self):
        while True:
            try:
                submission = Submission.query.get(int(self.redis_con.brpop(self.redis_key)[1]))
            except (ValueError, TypeError):
                continue
            if submission:
                submit_queue = self.queues.get(submission.oj_name)
                if submit_queue:
                    submit_queue.put(submission.id)
                else:
                    submission.verdict = 'Submit Failed'
                    db.session.commit()


class ProblemQueueHandler(threading.Thread):
    def __init__(self, queues, pool=None, daemon=None):
        super().__init__(daemon=daemon)
        self.redis_key = REDIS_CONFIG['queue']['problem_queue']
        self.redis_con = redis.StrictRedis(connection_pool=pool)
        self.queues = queues

    def run(self):
        while True:
            result = self.redis_con.brpop(self.redis_key, timeout=3600)
            if result:
                try:
                    data = json.loads(result[1].decode())
                    oj_name = data.get('oj_name')
                    problem_id = data.get('problem_id')
                except json.decoder.JSONDecodeError:
                    continue
                que = self.queues.get(oj_name)
                if que:
                    que.put(problem_id)
            else:
                self.refresh_problem()

    def refresh_problem(self):
        outdated_problems = Problem.query.filter(
            datetime.utcnow() - timedelta(days=1) > Problem.last_update).all()
        for p in outdated_problems:
            que = self.queues.get(p.oj_name)
            if que:
                que.put(p.problem_id)
        result = db.session.query(
            Problem.oj_name.label('oj_name'),
            func.max(Problem.problem_id).label('max_id')).group_by(
            Problem.oj_name).all()
        for oj_name, max_id in result:
            que = self.queues.get(oj_name)
            if que:
                for i in range(1, 21):
                    problem_id = str(int(max_id) + i)
                    que.put(problem_id)


class VJudge(object):
    def __init__(self):
        with open(OJ_CONFIG) as f:
            oj_config = json.load(f)
        self.accounts = oj_config['accounts']
        self.pool = redis.ConnectionPool(host=REDIS_CONFIG['host'], port=REDIS_CONFIG['port'])
        self.redis_con = redis.StrictRedis(connection_pool=self.pool)
        self.available_ojs = []
        self.submit_queues = {}
        self.status_queues = {}
        self.problem_queues = {}

    def start(self):
        for oj_name in self.accounts:
            self._add_judge(oj_name)
        if not self.available_ojs:
            logging.warning('there is no oj is available')
        else:
            logging.info('{} are available'.format(self.available_ojs))
        SubmitQueueHandler(self.submit_queues, pool=self.pool, daemon=True).start()
        ProblemQueueHandler(self.problem_queues, pool=self.pool, daemon=True).start()
        while True:
            time.sleep(3600)
            for oj_name in self.accounts:
                if oj_name not in self.available_ojs:
                    self._add_judge(oj_name)

    def _add_judge(self, oj_name):
        if oj_name not in self.accounts:
            return False
        available = False
        for username in self.accounts[oj_name]:
            password = self.accounts[oj_name][username]
            client = get_client(oj_name, auth=(username, password))
            if client is not None:
                logging.info("user '{}' log in to {} successfully".format(username, oj_name))
                if not available:
                    self.submit_queues[oj_name] = Queue()
                    self.problem_queues[oj_name] = Queue()
                    self.status_queues[oj_name] = Queue()
                    available = True
                Submitter(client, self.submit_queues.get(oj_name),
                          self.status_queues.get(oj_name), daemon=True).start()
        if available:
            self.available_ojs.append(oj_name)
            StatusCrawler(get_client(oj_name),
                          self.status_queues.get(oj_name), daemon=True).start()
            ProblemCrawler(get_client(oj_name),
                           self.problem_queues.get(oj_name), daemon=True).start()
        return available
