import asyncio
import threading
import json
import redis
from queue import Queue, Empty
from datetime import datetime, timedelta
from sqlalchemy import or_
from config import REDIS_CONFIG, logger
from .site import get_client_by_oj_name, supported_sites, exceptions
from .models import db, Submission, Problem, Contest


class StatusCrawler(threading.Thread):
    def __init__(self, client, daemon=None):
        super().__init__(daemon=daemon)
        self._client = client
        self._user_id = client.get_user_id()
        self._name = client.get_name()
        self._start_event = threading.Event()
        self._stop_event = threading.Event()
        self._tasks = []
        self._thread = None
        self._loop = None

    def run(self):
        self._thread = threading.current_thread()
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.call_soon(self._start_event.set)
        self._loop.run_forever()
        pending_tasks = asyncio.all_tasks(self._loop)
        self._loop.run_until_complete(asyncio.gather(*pending_tasks))

    def wait_start(self, timeout=None):
        return self._start_event.wait(timeout)

    def add_task(self, submission_id):
        if not self._start_event.is_set():
            raise RuntimeError('Cannot add task before crawler is started')
        if self._stop_event.is_set():
            raise RuntimeError('Cannot add task when crawler is stopping')
        self._loop.call_soon_threadsafe(
            asyncio.ensure_future, self._crawl_status(submission_id))
        return True

    def stop(self):
        if not self._start_event.is_set():
            raise RuntimeError('Cannot stop crawler before it is started')
        if self._stop_event.is_set():
            raise RuntimeError('Crawler can only be stopped once')
        self._stop_event.set()
        self._loop.call_soon_threadsafe(self._loop.stop)

    async def _crawl_status(self, submission_id):
        submission = Submission.query.get(submission_id)
        if (not submission.run_id or submission.oj_name != self._name
                or submission.verdict != 'Being Judged'):
            return
        for delay in range(120):
            await asyncio.sleep(delay)
            try:
                verdict, exe_time, exe_mem = self._client.get_submit_status(
                    submission.run_id,
                    user_id=submission.user_id,
                    problem_id=submission.problem_id)
            except exceptions.ConnectionError as e:
                submission.verdict = 'Judge Failed'
                db.session.commit()
                logger.error(f'Crawled status failed, submission_id: {submission.id}, reason: {e}')
                return
            except exceptions.LoginExpired:
                self._client.update_cookies()
                logger.debug(f'StatusCrawler login expired, login again, name: {self._name}, user_id: {self._user_id}')
                continue
            if verdict not in ('Being Judged', 'Queuing', 'Compiling', 'Running'):
                submission.verdict = verdict
                submission.exe_time = exe_time
                submission.exe_mem = exe_mem
                db.session.commit()
                logger.info(
                    f'Crawled status successfully, submission_id: {submission.id}, verdict: {submission.verdict}')
                return
        submission.verdict = 'Judge Failed'
        db.session.commit()
        logger.error(f'Crawled status failed, submission_id: {submission.id}, reason: Timeout')

    def __repr__(self):
        return f'<StatusCrawler(oj_name={self._name}, user_id={self._user_id})>'


class Submitter(threading.Thread):
    def __init__(self, client, submit_queue, status_crawler, daemon=None):
        super().__init__(daemon=daemon)
        self._client = client
        self._user_id = client.get_user_id()
        self._name = client.get_name()
        self._submit_queue = submit_queue
        self._status_crawler = status_crawler
        self._stop_event = threading.Event()

    def run(self):
        self._status_crawler.start()
        self._status_crawler.wait_start()
        logger.info(f'Started submitter, name: {self._name}, user_id: {self._user_id}')
        while True:
            try:
                submission = Submission.query.get(self._submit_queue.get(timeout=60))
            except Empty:
                if self._stop_event.is_set():
                    break
                continue
            if submission.verdict not in ('Queuing', 'Being Judged'):
                continue
            if submission.verdict == 'Being Judged':
                self._status_crawler.add_task(submission.id)
                continue
            try:
                run_id = self._client.submit_problem(
                    submission.problem_id, submission.language, submission.source_code)
            except (exceptions.SubmitError, exceptions.ConnectionError) as e:
                submission.verdict = 'Submit Failed'
                db.session.commit()
                logger.error(f'Submission {submission.id} is submitted failed, reason: {e}')
            except exceptions.LoginExpired:
                try:
                    self._client.update_cookies()
                    self._submit_queue.put(submission.id)
                    logger.debug(
                        f'Submitter login is expired, login again, name: {self._name}, user_id: {self._user_id}')
                except exceptions.ConnectionError as e:
                    submission.verdict = 'Submit Failed'
                    db.session.commit()
                    logger.error(f'Submission {submission.id} is submitted failed, reason: {e}')
            else:
                submission.run_id = run_id
                submission.user_id = self._user_id
                submission.verdict = 'Being Judged'
                db.session.commit()
                logger.info(f'Submission {submission.id} is submitted successfully')
                self._status_crawler.add_task(submission.id)
        logger.info(f'Stopping submitter, name: {self._name}, user_id: {self._user_id}')
        self._status_crawler.stop()
        self._status_crawler.join()
        logger.info(f'Stopped submitter, name: {self._name}, user_id: {self._user_id}')

    def stop(self):
        self._stop_event.set()

    def __repr__(self):
        return f'<Submitter(oj_name={self._name}, user_id={self._user_id})>'


class PageCrawler(threading.Thread):
    def __init__(self, client, page_queue, daemon=None):
        super().__init__(daemon=daemon)
        self._client = client
        self._name = client.get_name()
        self._user_id = client.get_user_id()
        self._page_queue = page_queue

    def run(self):
        pass


class SubmitterHandler(threading.Thread):
    def __init__(self, normal_accounts, contest_accounts, daemon=None):
        super().__init__(daemon=daemon)
        self._redis_key = REDIS_CONFIG['queue']['submitter_queue']
        self._redis_con = redis.StrictRedis(
            host=REDIS_CONFIG['host'], port=REDIS_CONFIG['port'], db=REDIS_CONFIG['db'])
        self._normal_accounts = normal_accounts
        self._contest_accounts = contest_accounts
        self._running_submitters = {}
        self._stopping_submitters = set()
        self._queues = {}

    def run(self):
        self._scan_unfinished_tasks()
        last_clean = datetime.utcnow()
        while True:
            data = self._redis_con.brpop(self._redis_key, timeout=600)
            if datetime.utcnow() - last_clean > timedelta(hours=1):
                self._clean_free_submitters()
            if not data:
                continue
            try:
                submission_id = int(data[1])
            except (ValueError, TypeError):
                logger.error(f'SubmitterHandler: receive corrupt data "{data[1]}"')
                continue
            submission = Submission.query.get(submission_id)
            if not submission:
                logger.error(f'Submission {submission_id} is not found')
                continue
            if submission.oj_name not in self._queues:
                self._queues[submission.oj_name] = Queue()
            submit_queue = self._queues.get(submission.oj_name)
            if submission.oj_name not in self._running_submitters:
                if not self._start_new_submitters(submission.oj_name, submit_queue):
                    submission.verdict = 'Submit Failed'
                    db.session.commit()
                    logger.error(f'Cannot start client for {submission.oj_name}')
                    continue
            assert submission.oj_name in self._running_submitters
            submit_queue.put(submission.id)

    def _scan_unfinished_tasks(self):
        submissions = Submission.query.filter(
            or_(Submission.verdict == 'Queuing', Submission.verdict == 'Being Judged'))
        for submission in submissions:
            self._redis_con.lpush(self._redis_key, submission.id)

    def _start_new_submitters(self, oj_name, submit_queue):
        submitter_info = {'submitters': {}}
        submitters = submitter_info.get('submitters')
        accounts = {}
        if oj_name in self._normal_accounts:
            accounts = self._normal_accounts[oj_name]
        if oj_name in self._contest_accounts:
            accounts = self._contest_accounts[oj_name]
        for auth in accounts:
            try:
                crawler = StatusCrawler(get_client_by_oj_name(oj_name, auth), daemon=True)
                submitter = Submitter(get_client_by_oj_name(oj_name, auth), submit_queue, crawler, daemon=True)
            except exceptions.JudgeException as e:
                logger.error(f'Create submitter failed, name: {oj_name}, user_id: auth[0], reason: {e}')
                continue
            submitter.start()
            submitters[auth[0]] = submitter
        if not submitters:
            return False
        submitter_info['start_time'] = datetime.utcnow()
        self._running_submitters[oj_name] = submitter_info
        return True

    def _clean_free_submitters(self):
        free_clients = []
        for oj_name in self._running_submitters:
            submitter_info = self._running_submitters[oj_name]
            if datetime.utcnow() - submitter_info['start_time'] > timedelta(hours=1):
                free_clients.append(oj_name)
        for oj_name in free_clients:
            submitter_info = self._running_submitters[oj_name]
            submitters = submitter_info.get('submitters')
            for user_id in submitters:
                submitter = submitters.get(user_id)
                submitter.stop()
                self._stopping_submitters.add(submitter)
            self._running_submitters.pop(oj_name)
            logger.info(f'No more task, stop all {oj_name} submitters')
        stopped_submitters = []
        for submitter in self._stopping_submitters:
            if not submitter.is_alive():
                stopped_submitters.append(submitter)
        for submitter in stopped_submitters:
            self._stopping_submitters.remove(submitter)
        logger.info('Cleaned free submitters')
        logger.info(f'Running submitters: {self._running_submitters}')
        logger.info(f'Stopping submitters: {self._stopping_submitters}')


class CrawlerHandler(threading.Thread):
    def __init__(self, normal_accounts, contest_accounts, daemon=None):
        super().__init__(daemon=daemon)
        self._redis_key = REDIS_CONFIG['queue']['crawler_queue']
        self._redis_con = redis.StrictRedis(
            host=REDIS_CONFIG['host'], port=REDIS_CONFIG['port'], db=REDIS_CONFIG['db'])
        self._normal_accounts = normal_accounts
        self._contest_accounts = contest_accounts

    def run(self):
        while True:
            data = self._redis_con.brpop(self._redis_key, timeout=600)
            if not data:
                continue
            try:
                data = json.loads(data[1])
            except json.JSONDecodeError:
                logger.error(f'CrawlerHandler: received corrupt data "{data[1]}"')
                continue
            crawl_type = data.get('type')
            oj_name = data.get('oj_name')
            if crawl_type not in ('problem', 'contest'):
                logger.error(f'Unsupported crawl_type: {crawl_type}')
                continue
            if oj_name not in self._normal_accounts or oj_name not in self._contest_accounts:
                logger.error(f'Unsupported oj_name: {oj_name}')
                continue
            if crawl_type == 'problem':
                crawl_all = data.get('all')
                problem_id = data.get('problem_id')
                if crawl_type is not True:
                    crawl_all = False
                if not crawl_all and problem_id is None:
                    logger.error('Missing crawl_params: problem_id')
                    continue
                self._handle_problem_crawling(oj_name, crawl_all, problem_id)
            elif crawl_type == 'contest':
                contest_id = data.get('contest_id')
                if contest_id is None:
                    logger.error('Missing crawl_params: contest_id')
                    continue
                self._handle_contest_crawling(oj_name, contest_id)

    def _handle_problem_crawling(self, oj_name, crawl_all=False, problem_id=None):
        pass

    def _handle_contest_crawling(self, oj_name, contest_id):
        pass


class VJudge(object):
    def __init__(self, normal_accounts=None, contest_accounts=None):
        if not normal_accounts and not contest_accounts:
            logger.warning('Neither normal_accounts nor contest_accounts has available account, '
                           'submitter and crawler will not work')
        self._normal_accounts = normal_accounts or {}
        self._contest_accounts = contest_accounts or {}

    @property
    def normal_accounts(self):
        return self._normal_accounts

    @property
    def contest_accounts(self):
        return self._contest_accounts

    def start(self):
        submitter_handle = SubmitterHandler(self._normal_accounts, self._contest_accounts, True)
        crawler_handle = CrawlerHandler(self._normal_accounts, self._contest_accounts, True)
        submitter_handle.start()
        crawler_handle.start()
        submitter_handle.join()
        crawler_handle.join()
