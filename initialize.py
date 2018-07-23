import logging
from datetime import datetime

from vjudge.site import exceptions, get_normal_client
from vjudge.models import db, Problem

logging.basicConfig(level=logging.INFO)


# TODO: remove crawler in the future
def crawler(site):
    client = get_normal_client(site)
    try:
        problem_list = client.get_problem_list()
    except exceptions.ConnectionError:
        return
    for problem_id in problem_list:
        try:
            result = client.get_problem(problem_id)
        except exceptions.ConnectionError:
            continue
        if result:
            p = Problem.query.filter_by(oj_name=site, problem_id=problem_id).first() or Problem()
            for attr in result:
                if hasattr(p, attr):
                    setattr(p, attr, result[attr])
            p.oj_name = site
            p.problem_id = problem_id
            p.last_update = datetime.utcnow()
            db.session.add(p)
            db.session.commit()
            logging.info('problem update: {}'.format(p.summary()))


def main():
    crawler('scu')
    crawler('hdu')


if __name__ == '__main__':
    main()
