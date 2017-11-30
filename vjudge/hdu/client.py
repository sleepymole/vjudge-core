import re

import requests
from bs4 import BeautifulSoup
from bs4.element import NavigableString

from .. import exceptions
from ..base import BaseClient

base_url = 'http://acm.hdu.edu.cn'

language_id = {'G++': '0', 'GCC': '1', 'C++': '2',
               'C': '3', 'Pascal': '4', 'Java': '5', 'C#': '6'}

titles = ['Problem Description', 'Input', 'Output', 'Sample Input', 'Sample Output']
page_titles = {'Problem Description': 'description', 'Input': 'input', 'Output': 'output',
               'Sample Input': 'sample_input', 'Sample Output': 'sample_output'}


class HDUClient(BaseClient):
    def __init__(self, auth=None, **kwargs):
        super().__init__()
        self.name = 'hdu'
        if 'contest_id' in kwargs:
            self.client_type = 'contest'
            self.contest_id = kwargs['contest_id']
        else:
            self.client_type = 'practice'
        self.auth = auth
        self.timeout = kwargs.get('timeout', 5)
        if auth is not None:
            self.username, self.password = auth
            self.login(self.username, self.password)

    def get_name(self):
        return self.name

    def login(self, username, password):
        url = self._get_login_url()
        data = {
            'login': 'Sign in',
            'username': username,
            'userpass': password
        }
        try:
            r = self._session.post(url, data, timeout=self.timeout)
        except requests.exceptions.RequestException:
            raise exceptions.ConnectionError
        if re.search('Sign In Your Account', r.text):
            raise exceptions.LoginError
        self.auth = (username, password)
        self.username = username
        self.password = password

    def check_login(self):
        url = base_url + '/control_panel.php'
        try:
            r = self._session.get(url, timeout=self.timeout)
        except requests.exceptions.RequestException:
            raise exceptions.ConnectionError
        if re.search('Sign In Your Account', r.text):
            return False
        return True

    def get_user_id(self):
        return self.username

    def update_cookies(self):
        if self.auth is None:
            raise exceptions.LoginRequired
        self.login(self.username, self.password)

    def get_problem(self, problem_id):
        url = self._get_problem_url(problem_id)
        try:
            r = self._session.get(url)
        except requests.exceptions.RequestException:
            raise exceptions.ConnectionError
        return self._parse_problem(r.text)

    def submit_problem(self, problem_id, language, source_code):
        if self.auth is None:
            raise exceptions.LoginRequired
        if language not in language_id:
            raise exceptions.SubmitError
        data = {
            'problemid': problem_id,
            'language': language_id[language],
            'usercode': source_code
        }
        if self.client_type == 'contest':
            data['submit'] = 'Submit'
        else:
            data['check'] = '0'
        url = self._get_submit_url()
        try:
            r = self._session.post(url, data, timeout=self.timeout)
            r.encoding = 'GBK'
            if re.search('Sign In Your Account', r.text):
                raise exceptions.LoginExpired
            if not re.search('Realtime Status', r.text):
                raise exceptions.SubmitError
            status_url = self._get_status_url(problem_id=problem_id, user_id=self.username)
            r = self._session.get(status_url, timeout=self.timeout)
        except requests.exceptions.RequestException:
            raise exceptions.ConnectionError
        try:
            table = BeautifulSoup(r.text, 'lxml').find('div', id='fixed_table').table
            run_id = next(table.find('tr', align="center").stripped_strings)
        except (AttributeError, StopIteration):
            raise exceptions.SubmitError
        return run_id

    def get_submit_status(self, run_id, **kwargs):
        if self.client_type == 'contest':
            raise exceptions.LoginRequired
        user_id = kwargs.get('user_id', '')
        problem_id = kwargs.get('problem_id', '')
        url = self._get_status_url(run_id=run_id, problem_id=problem_id, user_id=user_id)
        try:
            r = self._session.get(url, timeout=self.timeout)
            r.encoding = 'GBK'
        except requests.exceptions.RequestException:
            raise exceptions.ConnectionError
        if re.search('Sign In Your Account', r.text):
            raise exceptions.LoginExpired
        result = self._find_verdict(r.text, run_id)
        if result is not None:
            return result
        if self.client_type == 'contest':
            for page in range(2, 5):
                status_url = url + '&page={}'.format(page)
                try:
                    r = self._session.get(status_url, timeout=self.timeout)
                    r.encoding = 'GBK'
                except requests.exceptions.RequestException:
                    raise exceptions.ConnectionError
                result = self._find_verdict(r.text, run_id)
                if result is not None:
                    return result

    def _get_login_url(self):
        login_url = base_url + '/userloginex.php?action=login'
        if self.client_type == 'contest': login_url += '&cid={}&notice=0'.format(self.contest_id)
        return login_url

    def _get_submit_url(self):
        if self.client_type == 'contest':
            return base_url + '/contests/contest_submit.php?action=submit&cid={}'.format(self.contest_id)
        else:
            return base_url + '/submit.php?action=submit'

    def _get_status_url(self, run_id='', problem_id='', user_id=''):
        if self.client_type == 'contest':
            return base_url + '/contests/contest_status.php?cid={}&pid={}&user={}&lang=0&status=0'. \
                format(self.contest_id, problem_id, user_id)
        else:
            return base_url + '/status.php?first={}&pid={}&user={}&lang=0&status=0'. \
                format(run_id, problem_id, user_id)

    def _get_problem_url(self, problem_id):
        if self.client_type == 'contest':
            return base_url + '/contests/contest_showproblem.php?pid={}&cid={}'. \
                format(problem_id, self.contest_id)
        else:
            return base_url + '/showproblem.php?pid={}'.format(problem_id)

    @staticmethod
    def _parse_problem(text):
        result = {}
        pattern = r'Time Limit:.*?[0-9]*/([0-9]*).*?MS.*?\(Java/Others\).*?' \
                  'Memory Limit:.*?[0-9]*/([0-9]*).*?K.*?\(Java/Others\)'
        limit = re.search(pattern, text)
        text = text.replace('src=../../../data/images', 'src=http://acm.hdu.edu.cn/data/images')
        if limit:
            result['time_limit'] = limit.group(1)
            result['mem_limit'] = limit.group(2)
        soup = BeautifulSoup(text, 'lxml')
        if soup.h1:
            result['title'] = soup.h1.text
            if result['title'] == 'System Message':
                return
        tags = soup.find_all('div', 'panel_title', align='left')
        index = 0
        for title in titles:
            while index < len(tags) and tags[index].string != title:
                index += 1
            if index >= len(tags):
                break
            tag = tags[index].next_sibling
            limit = 0
            while tag is not None and type(tag) is NavigableString and limit < 3:
                tag = tag.next_sibling
                limit += 1
            items = []
            for i in tag.contents:
                if type(i) is NavigableString:
                    items.append(str(i))
                else:
                    items.append(i.prettify())
            result[page_titles[title]] = ''.join(items)
        return result

    @staticmethod
    def _find_verdict(response, run_id):
        try:
            table = BeautifulSoup(response, 'lxml').find('div', id='fixed_table').table
            tags = table.find_all('tr', align="center")
            for tag in tags:
                result = [x.text for x in tag.find_all('td')]
                if result[0] == run_id:
                    verdict = result[2]
                    exe_time = int(result[4].replace('MS', ''))
                    exe_mem = int(result[5].replace('K', ''))
                    if re.search('Runtime Error', verdict):
                        verdict = 'Runtime Error'
                    return verdict, exe_time, exe_mem
        except (AttributeError, IndexError, ValueError):
            pass

    @staticmethod
    def _encode_source_code(code):
        from urllib import parse
        import base64
        return base64.b64encode(parse.quote(code).encode('utf-8')).decode('utf-8')
