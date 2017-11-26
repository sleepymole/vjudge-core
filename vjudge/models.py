from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime
from . import db


class Submission(db.Model):
    __tablename__ = 'submissions'
    id = Column(Integer, primary_key=True)
    user_id = Column(String, index=True)
    oj_name = Column(String, nullable=False)
    problem_id = Column(String, nullable=False)
    language = Column(String, nullable=False)
    source_code = Column(String, nullable=False)
    run_id = Column(String)
    verdict = Column(String, default='Queuing')
    exe_time = Column(Integer)
    exe_mem = Column(Integer)
    time_stamp = Column(DateTime, default=datetime.utcnow)

    def to_json(self):
        json_submission = {
            'id': self.id,
            'oj_name': self.oj_name,
            'problem_id': self.problem_id,
            'verdict': self.verdict,
            'exe_time': self.exe_time,
            'exe_mem': self.exe_mem
        }
        return json_submission

    def __repr__(self):
        return '<Submission(id={}, user_id={}, oj_name={}, problem_id={} verdict={})>'. \
            format(self.run_id, self.user_id, self.oj_name, self.problem_id, self.verdict)


class Problem(db.Model):
    __tablename__ = 'problems'
    oj_name = Column(String, primary_key=True, index=True)
    problem_id = Column(String, primary_key=True, index=True)
    last_update = Column(DateTime, nullable=False)
    title = Column(String)
    description = Column(String)
    input = Column(String)
    output = Column(String)
    sample_input = Column(String)
    sample_output = Column(String)
    time_limit = Column(Integer)
    mem_limit = Column(Integer)

    def to_json(self):
        problem_json = {
            'oj_name': self.oj_name,
            'problem_id': self.problem_id,
            'title': self.title,
            'description': self.description,
            'input': self.input,
            'output': self.output,
            'sample_input': self.sample_input,
            'sample_output': self.sample_output,
            'time_limit': self.time_limit,
            'mem_limit': self.mem_limit
        }
        return problem_json

    def summary(self):
        summary_json = {
            'oj_name': self.oj_name,
            'problem_id': self.problem_id,
            'title': self.title,
        }
        return summary_json

    def __repr__(self):
        return '<Problem<{} {}: {}>'.format(self.oj_name, self.problem_id, self.title)
