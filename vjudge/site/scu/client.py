import os
import re
import sqlite3

import requests
from bs4 import BeautifulSoup

from .. import exceptions
from ..base import BaseClient

__all__ = ('SOJClient',)

base_url = 'http://acm.scu.edu.cn/soj'
base_dir = os.path.abspath(os.path.dirname(__file__))
db = sqlite3.connect(os.path.join(base_dir, 'captcha.db'), check_same_thread=False)


class SOJClient(BaseClient):
    def __init__(self, auth=None, **kwargs):
        super().__init__()
        self.auth = auth
        self.name = 'scu'
        self.timeout = kwargs.get('timeout', 5)
        if auth is not None:
            self.username, self.password = auth
            self.login(self.username, self.password)

    def get_name(self):
        return self.name

    def get_user_id(self):
        if self.auth is None:
            raise exceptions.LoginRequired
        return self.username

    def login(self, username, password):
        url = base_url + '/login.action'
        data = {
            'back': 2,
            'id': username,
            'password': password,
            'submit': 'login'
        }
        try:
            r = self._session.post(url, data, timeout=self.timeout)
        except requests.exceptions.ConnectionError:
            raise exceptions.ConnectionError
        if re.search('USER_NOT_EXIST', r.text):
            raise exceptions.UserNotExist
        elif re.search('PASSWORD_ERROR', r.text):
            raise exceptions.PasswordError
        self.auth = (username, password)
        self.username = username
        self.password = password

    def check_login(self):
        url = f'{base_url}/update_user_form.action'
        try:
            r = self._session.get(url, timeout=self.timeout)
        except requests.exceptions.RequestException:
            raise exceptions.ConnectionError
        if re.search('Please login first', r.text):
            return False
        return True

    def update_cookies(self):
        if self.auth is None:
            raise exceptions.LoginRequired
        self.login(self.username, self.password)

    def get_problem(self, problem_id):
        url = f'{base_url}/problem.action?id={problem_id}'
        try:
            r = self._session.get(url, timeout=self.timeout)
        except requests.exceptions.RequestException:
            raise exceptions.ConnectionError
        if re.search('No such problem', r.text):
            return
        try:
            title = re.findall('<title>{}: (.*?)</title>'.format(problem_id), r.text)[0]
        except IndexError:
            return
        return {'title': title}

    def get_problem_list(self):
        url = f'{base_url}/problems.action'
        try:
            r = self._session.get(url, timeout=self.timeout)
        except requests.exceptions.RequestException:
            raise exceptions.ConnectionError
        volume_list = []
        try:
            table = BeautifulSoup(r.text, 'lxml').find('table')
            tr = table.find('tr')
            tr = tr.find_next_sibling('tr')
            tags = tr.find_all('a')
            for tag in tags:
                r = re.search(r'\[(.*)\]', tag.text.strip())
                volume_list.append(r.groups()[0])
        except (AttributeError, IndexError):
            pass
        problem_list = []
        for vol in volume_list:
            page_url = f'{url}?volume={vol}'
            try:
                r = self._session.get(page_url, timeout=self.timeout)
            except requests.exceptions.RequestException:
                raise exceptions.ConnectionError
            problem_list += self.__class__._parse_problem_id(r.text)
        problem_list.sort()
        return problem_list

    def submit_problem(self, problem_id, language, source_code):
        if self.auth is None:
            raise exceptions.LoginRequired
        submit_url = f'{base_url}/submit.action'
        status_url = f'{base_url}/solutions.action?userId={self.username}&problemId={problem_id}'
        captcha = self._get_captcha()
        if captcha is None:
            raise exceptions.JudgeException('Can not find a valid captcha')
        data = {
            'problemId': problem_id,
            'validation': captcha,
            'language': language,
            'source': source_code,
            'submit': 'Submit'
        }
        try:
            r = self._session.post(submit_url, data, timeout=self.timeout)
            if re.search('ERROR', r.text):
                if not self.check_login():
                    raise exceptions.LoginExpired
                else:
                    raise exceptions.SubmitError
            r = self._session.get(status_url, timeout=self.timeout)
        except requests.exceptions.RequestException:
            raise exceptions.ConnectionError
        soup = BeautifulSoup(r.text, 'lxml')
        try:
            tag = soup.find_all('table')[1].find_all('tr')[1]
            run_id = next(tag.stripped_strings)
        except IndexError:
            raise exceptions.SubmitError
        return run_id

    def get_submit_status(self, run_id, **kwargs):
        status_url = f'{base_url}/solutions.action?from={run_id}'
        try:
            r = self._session.get(status_url, timeout=self.timeout)
        except requests.exceptions.RequestException:
            raise exceptions.ConnectionError
        try:
            soup = BeautifulSoup(r.text, 'lxml')
            tag = soup.find_all('table')[1].find_all('tr')[1]
            col_tags = tag.find_all('td')
            result = [' '.join(x.stripped_strings) for x in col_tags[5:]]
            verdict, exe_time, exe_mem = result[0], int(result[1]), int(result[2])
            return verdict, exe_time, exe_mem
        except (IndexError, ValueError):
            pass

    @staticmethod
    def _parse_problem_id(text):
        ids = []
        table = BeautifulSoup(text, 'lxml').find('table')
        if not table:
            return ids
        trs = table.find_all('tr')[3:]
        for tr in trs:
            try:
                tds = tr.find_all('td')
                pid = tds[1].text.strip()
                int(pid)
            except (ValueError, IndexError):
                continue
            ids.append(pid)
        return ids

    def _get_captcha(self):
        url = os.path.join(base_url, 'validation_code')
        try:
            r = self._session.get(url, timeout=self.timeout)
        except requests.exceptions.RequestException:
            raise exceptions.ConnectionError
        import hashlib
        h = hashlib.md5(r.content).hexdigest()
        cursor = db.cursor()
        cursor.execute("SELECT Code FROM Captcha WHERE Hash=?", (h,))
        res = cursor.fetchall()
        try:
            return res[0][0]
        except IndexError:
            return
