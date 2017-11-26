import logging
from vjudge.models import db, Problem
from vjudge.scu.client import SOJClient
from vjudge.hdu.client import HDUClient
from vjudge import exceptions
from datetime import datetime

logging.basicConfig(level=logging.INFO)


def crawler(oj_name, client, start, end):
    for problem_id in range(start, end):
        try:
            result = client.get_problem(problem_id)
        except exceptions.ConnectionError:
            continue
        if result:
            p = Problem.query.filter_by(oj_name=oj_name, problem_id=problem_id).first() or Problem()
            for attr in result:
                if hasattr(p, attr):
                    setattr(p, attr, result[attr])
            p.oj_name = oj_name
            p.problem_id = problem_id
            p.last_update = datetime.utcnow()
            db.session.add(p)
            db.session.commit()
            logging.info('problem update: {}'.format(p.summary()))


crawler('scu', SOJClient(), 1000, 5000)
crawler('hdu', HDUClient(), 1000, 7000)
