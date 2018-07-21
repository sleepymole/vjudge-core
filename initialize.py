import os
import logging
from datetime import datetime

from vjudge.site import exceptions, HDUClient, SOJClient
from vjudge.models import db, Problem

logging.basicConfig(level=logging.INFO)


def init_db():
    db.create_all()


def crawler(oj_name, client, start, end):
    for i in range(start, end):
        problem_id = str(i)
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


def main():
    init_db()
    crawler('scu', SOJClient(), 1000, 5000)
    crawler('hdu', HDUClient(), 1000, 7000)


if __name__ == '__main__':
    main()
