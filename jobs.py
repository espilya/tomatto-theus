import json
from datetime import datetime

class Job:
    def __init__(self, job_id, user, job_name, alloc_nodes, date_submit, date_start, date_end, state, host):
        self.job_id = job_id
        self.user = user
        self.job_name = job_name
        self.alloc_nodes = alloc_nodes
        self.date_submit = date_submit
        self.date_start = date_start
        self.date_end = date_end
        self.host = host
        self.state = state

    def update_state(self, new_state):
        if self.state != new_state:
            self.state = new_state
            return True

    def __repr__(self):
        return f"Job(job_id='{self.job_id}', user='{self.user}', job_name='{self.job_name}', alloc_nodes='{self.alloc_nodes}', date_submit='{self.date_submit}', date_start='{self.date_start}', date_end='{self.date_end}', state='{self.state}', host='{self.host}')"

    def to_dict(self):
        """
        Convert the Job object into a dictionary that can be serialized to JSON.
        """
        return {
            'job_id': self.job_id,
            'user': self.user,
            'job_name': self.job_name,
            'alloc_nodes': self.alloc_nodes,
            'date_submit': self.date_submit.isoformat() if isinstance(self.date_submit, datetime) else self.date_submit,
            'date_start': self.date_start.isoformat() if isinstance(self.date_start, datetime) else self.date_start,
            'date_end': self.date_end.isoformat() if isinstance(self.date_end, datetime) else self.date_end,
            'state': self.state,
            'host': self.host
        }

    def to_json(self):
        """
        Convert the Job object into a JSON string using its dictionary representation.
        """
        return self.to_dict()
        # return json.dumps(self.to_dict())


class JobManager:
    def __init__(self):
        self.pending_jobs = []
        self.running_jobs = []
        self.finished_jobs = []

    def new_cycle(self):
        self.pending_jobs = []
        self.running_jobs = []
        self.finished_jobs = []

    def add_pending_job(self, job):
        self.pending_jobs.append(job)

    def add_running_job(self, job):
        self.running_jobs.append(job)

    def add_finished_job(self, job):
        self.finished_jobs.append(job)

    # def add_job(self, job_id, user, job_name, alloc_nodes, date_submit, date_start, date_end, state, host):
    #     job = Job(job_id, user, job_name, alloc_nodes, date_submit, date_start, date_end, state, host)
    #     self.jobs.append(job)

    def to_json(self, jobs):
        result = []
        for i in jobs:
            result.append(i.to_json())
        return result

    def get_all(self):
        return self.pending_jobs +  self.running_jobs +  self.finished_jobs

    def get_pending_jobs(self):
        pending_jobs = [job for job in self.pending_jobs]
        return pending_jobs

    def get_running_jobs(self):
        running_jobs = [job for job in self.running_jobs]
        return running_jobs

    def get_running_jobs_host(self):
        hosts = [job.host for job in self.running_jobs]
        return hosts

    def get_finished_jobs(self):
#        pending_jobs = [job for job in self.finished_jobs if job.state not in ['RUNNING', 'PENDING']]
        return self.finished_jobs

    def mark_job_completed(self, job):
        self.remove_job(job)
        self.finished_jobs.append(job)

    def update_job_state(self, job_id, new_state):
        changed = False
        for job in self.jobs:
            if job.job_id == job_id:
                changed = job.update_state(new_state)
                return changed

    # def remove_job(r_job):
    #     self.jobs = [job for job in self.jobs if job.job_id != r_job.job_id]

    def job_exists(self, job_id):
        if self.find_job_by_id(job_id) is None:
            return False
        else:
            return True

    def find_job_by_id(self, job_id):
        jobs =  self.pending_jobs +  self.running_jobs +  self.finished_jobs
        found_jobs = [job for job in jobs if job.job_id == job_id]
        return found_jobs[0] if found_jobs else None

    # def display_jobs(self):
    #     for job in self.jobs:
    #         print(job)

