import json
from datetime import datetime, timedelta

import redis
from flask import Flask, jsonify, request, abort, url_for
from sqlalchemy import and_

from config import REDIS_CONFIG
from vjudge.models import db, Submission, Problem

app = Flask(__name__)

redis_con = redis.StrictRedis(host=REDIS_CONFIG['host'], port=REDIS_CONFIG['port'])
submit_queue = REDIS_CONFIG['queue']['submit_queue']
problem_queue = REDIS_CONFIG['queue']['problem_queue']


@app.route('/problems/')
def get_problems():
    page = request.args.get('page', 1, type=int)
    oj_name = request.args.get('oj_name', '%')
    problem_id = request.args.get('problem_id', '%')
    pagination = Problem.query.filter(
        and_(Problem.oj_name.like(oj_name),
             Problem.problem_id.like(problem_id))). \
        paginate(page=page, error_out=False)
    problems = pagination.items
    page = pagination.page
    prev = None
    if pagination.has_prev:
        prev = url_for('get_problems', page=page - 1, _external=True)
    next = None
    if pagination.has_next:
        next = url_for('get_problems', page=page + 1, _external=True)
    return jsonify({
        'problems': [p.summary() for p in problems],
        'prev': prev,
        'next': next,
        'count': pagination.total
    })


@app.route('/problems/<oj_name>/<problem_id>')
def get_problem(oj_name, problem_id):
    problem = Problem.query.filter_by(oj_name=oj_name, problem_id=problem_id).first()
    if problem is None:
        abort(404)
    if datetime.utcnow() - timedelta(days=1) > problem.last_update:
        redis_con.rpush(problem_queue, json.dumps({'oj_name': oj_name, 'problem_id': problem_id}))
    return jsonify(problem.to_json())


@app.route('/problems/<oj_name>/<problem_id>', methods=['POST'])
def update_problem(oj_name, problem_id):
    redis_con.rpush(problem_queue, json.dumps({'oj_name': oj_name, 'problem_id': problem_id}))
    return jsonify({'url': url_for('get_problem', oj_name=oj_name, problem_id=problem_id)})


@app.route('/problems/', methods=['POST'])
def submit_problem():
    oj_name = request.form.get('oj_name')
    problem_id = request.form.get('problem_id')
    language = request.form.get('language')
    source_code = request.form.get('source_code')
    if None in (oj_name, problem_id, language, source_code):
        return jsonify({'error': 'parameter error'})
    if not Problem.query.filter_by(oj_name=oj_name, problem_id=problem_id).first():
        return jsonify({'error': 'no such problem'})
    submission = Submission(oj_name=oj_name, problem_id=problem_id,
                            language=language, source_code=source_code)
    db.session.add(submission)
    db.session.commit()
    redis_con.rpush(submit_queue, submission.id)
    url = url_for('get_submission', id=submission.id, _external=True)
    return jsonify({'id': submission.id, 'url': url})


@app.route('/submissions/<id>')
def get_submission(id):
    submission = Submission.query.get(id)
    if submission is None:
        abort(404)
    return jsonify(submission.to_json())


@app.teardown_appcontext
def shutdown_session(response_or_exc):
    db.session.remove()
    return response_or_exc


@app.errorhandler(404)
def page_not_found(e):
    return jsonify({'error': 'not found'}), 404
