#!/usr/bin/env python3
from flask_script import Manager, Shell
from vjudge.models import Submission, Problem
from server import app, submit_queue, crawl_queue
from vjudge.base import VJudge
from vjudge import db


def make_shell_context():
    return dict(app=app, db=db, Submission=Submission, Problem=Problem)


manager = Manager(app)
manager.add_command('shell', Shell(make_context=make_shell_context))

vjudge = VJudge(submit_queue, crawl_queue)
vjudge.setDaemon(True)
vjudge.start()

if __name__ == '__main__':
    manager.run()
