import shlex
import subprocess

from config import get_accounts
from vjudge.main import VJudge

p = subprocess.Popen(shlex.split("gunicorn -w 2 -k gevent -b 'localhost:5000' manage:app"))

try:
    normal_accounts, contest_accounts = get_accounts()
    vjudge = VJudge(normal_accounts=normal_accounts, contest_accounts=contest_accounts)
    vjudge.start()
finally:
    p.terminate()
    p.wait()
