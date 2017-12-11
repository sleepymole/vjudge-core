import shlex
import subprocess

from vjudge.main import VJudge

subprocess.Popen(shlex.split("gunicorn -w 2 -k gevent -b 'localhost:5000' manage:app"))

vjudge = VJudge()

vjudge.start()
