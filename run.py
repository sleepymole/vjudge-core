import shlex
import subprocess

from vjudge.main import VJudge

p = subprocess.Popen(shlex.split("gunicorn -w 2 -k gevent -b 'localhost:5000' manage:app"))

try:
    vjudge = VJudge()
    vjudge.start()
finally:
    p.terminate()
    p.wait()
