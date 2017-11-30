import shlex
import subprocess

from vjudge.main import VJudge

subprocess.Popen(shlex.split('gunicorn -w 2 --threads 10 manage:app'))

vjudge = VJudge()

vjudge.start()
