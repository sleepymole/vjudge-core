import asyncio
import threading
import re
import redis
from queue import Queue, Empty
from datetime import datetime, timedelta
from config import REDIS_CONFIG, logger
from .site import get_normal_client, get_contest_client, exceptions
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
                logger.error(f'Crawled status for {submission} failed. Reason: {e}')
                return
            except exceptions.LoginExpired:
                self._client.update_cookies()
                logger.debug(f'{self}: Login is expired, login again')
                continue
            if verdict not in ('Being Judged', 'Queuing', 'Compiling', 'Running'):
                submission.verdict = verdict
                submission.exe_time = exe_time
                submission.exe_mem = exe_mem
                db.session.commit()
                logger.info(f'Crawled status for {submission} successfully')
                return
        submission.verdict = 'Judge Failed'
        db.session.commit()
        logger.error(f'Crawled status for {submission} failed. Reason: Timeout')

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
        logger.info(f'{self} started successfully')
        while True:
            try:
                submission = Submission.query.get(self._submit_queue.get(timeout=60))
            except Empty:
                if self._stop_event.is_set():
                    break
                continue
            try:
                run_id = self._client.submit_problem(
                    submission.problem_id, submission.language, submission.source_code)
            except (exceptions.SubmitError, exceptions.ConnectionError) as e:
                submission.verdict = 'Submit Failed'
                db.session.commit()
                logger.error(f'{submission} is submitted failed. Reason: {e}')
            except exceptions.LoginExpired:
                try:
                    self._client.update_cookies()
                    self._submit_queue.put(submission.id)
                    logger.debug(f'{self}: Login is expired, login again')
                except exceptions.ConnectionError as e:
                    submission.verdict = 'Submit Failed'
                    db.session.commit()
                    logger.error(f'{submission} is submitted failed. Reason: {e}')
            else:
                submission.run_id = run_id
                submission.user_id = self._user_id
                submission.verdict = 'Being Judged'
                db.session.commit()
                logger.info(f'{submission} is submitted successfully')
                self._status_crawler.add_task(submission.id)
        self._status_crawler.stop()
        self._status_crawler.join()

    def stop(self):
        self._stop_event.set()

    def __repr__(self):
        return f'<Submitter(oj_name={self._name}, user_id={self._user_id})>'


class PageCrawler(threading.Thread):
    def __init__(self, client, page_queue, daemon=None):
        super().__init__(daemon=daemon)
        self._client = client
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
        self._stopping_submitters = {}
        self._queues = {}

    def run(self):
        last_clean = datetime.utcnow()
        while True:
            data = self._redis_con.brpop(self._redis_key, timeout=600)
            if datetime.utcnow() - last_clean > timedelta(minutes=10):
                self._clean_free_clients()
            if not data:
                continue
            try:
                submission_id = int(data[1])
            except (ValueError, TypeError):
                logger.error(f'Receive corrupt data "{data[1]}"')
                continue
            submission = Submission.query.get(submission_id)
            if not submission:
                logger.error(f'Submission {submission_id} is not found')
                continue
            if submission.oj_name not in self._queues:
                self._queues[submission.oj_name] = Queue()
            submit_queue = self._queues.get(submission.oj_name)
            if submission.oj_name not in self._running_submitters:
                if not self._start_new_clients(submission.oj_name, submit_queue):
                    submission.verdict = 'Submit Failed'
                    db.session.commit()
                    logger.error(f'Cannot start client for {submission.oj_name}')
                    continue
            assert submission.oj_name in self._running_submitters
            submit_queue.put(submission.id)

    def _start_new_clients(self, oj_name, submit_queue):
        submitter_info = {'submitters': {}}
        submitters = submitter_info.get('submitters')
        if oj_name in self._normal_accounts:
            accounts = self._normal_accounts[oj_name]
            for auth in accounts:
                try:
                    crawler = StatusCrawler(get_normal_client(oj_name, auth), daemon=True)
                    submitter = Submitter(get_normal_client(oj_name, auth), submit_queue, crawler, daemon=True)
                except exceptions.JudgeException as e:
                    logger.error(f'Create submitter for {oj_name}:{auth} failed. Reason: {e}')
                    continue
                submitter.start()
                submitters[auth[0]] = submitter
        elif oj_name in self._contest_accounts:
            accounts = self._contest_accounts[oj_name]
            res = re.match(r'^.*?_ct_([0-9]+)$', oj_name)
            if not res:
                return False
            site, contest_id = res
            for auth in accounts:
                try:
                    crawler = StatusCrawler(get_contest_client(site, auth, contest_id), daemon=True)
                    submitter = Submitter(
                        get_contest_client(site, auth, contest_id), submit_queue, crawler, daemon=True)
                except exceptions.JudgeException as e:
                    logger.error(f'Create submitter for {oj_name}:{auth} failed. Reason: {e}')
                    continue
                submitter.start()
                submitters[auth[0]] = submitter
        if not submitters:
            return False
        submitter_info['start_time'] = datetime.utcnow()
        self._running_submitters[oj_name] = submitter_info
        return True

    def _clean_free_clients(self):
        pass


class CrawlerHandler(threading.Thread):
    def __init__(self, normal_accounts, contest_accounts, daemon=None):
        super().__init__(daemon=daemon)
        self._redis_con = redis.StrictRedis(
            host=REDIS_CONFIG['host'], port=REDIS_CONFIG['port'], db=REDIS_CONFIG['db'])
        self._normal_accounts = normal_accounts
        self._contest_accounts = contest_accounts

    def run(self):
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
