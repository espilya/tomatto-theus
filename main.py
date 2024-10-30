import sys
import os
import random
import time
from prometheus_client import start_http_server, Gauge
import subprocess
from datetime import datetime
import traceback
import threading

from reader import *
from jobs import *
from flask import *

# prometheus metrics
job_cpu_gauge = Gauge('job_cpu_usage_percent', 'CPU usage', ['jobid', 'user', 'jobname', 'hostname'])
job_mem_gauge = Gauge('job_memory_usage_percent', 'Memory usage', ['jobid', 'user', 'jobname', 'hostname'])
job_date_submit = Gauge('job_date_submit', 'Submit date', ['jobid', 'user', 'jobname', 'hostname'])
job_date_start = Gauge('job_date_start', 'Start date', ['jobid', 'user', 'jobname', 'hostname'])
job_date_end = Gauge('job_date_end', 'End date', ['jobid', 'user', 'jobname', 'hostname'])

global jobsManager
jobsManager = JobManager()
app = Flask(__name__)

def _print(*args, **kw):
    print("[%s]" % (datetime.now()),*args, **kw)

def read_jobs():
    #  sacct --format=JobID,user,jobname,allocnodes,submit,start,end,state,NodeList -nPa
    cmd=['sacct', '--format=JobID,user,jobname,allocnodes,submit,start,end,state,NodeList', '-nPa']
    res = subprocess.run(cmd, stdout=subprocess.PIPE)
    res = res.stdout.decode('utf-8')
    lines = res.strip().split('\n')
#    print(lines)
#    print(jobsManager.get_all())
    jobsManager.new_cycle()
    for i in lines:
        fields = i.split('|')
        if all(c.isdigit() or c in '_-[]' for c in fields[0]):
            job = Job(job_id=fields[0], user=fields[1], job_name=fields[2], alloc_nodes=fields[3], date_submit=fields[4], date_start=fields[5], date_end=fields[6], state=fields[7], host=fields[8])
            # _print("New job added:", job)
            if fields[7] == 'PENDING':
                jobsManager.add_pending_job(job)
            elif fields[7] == 'RUNNING':
                jobsManager.add_running_job(job)
            elif fields[7] not in ['RUNNING', 'PENDING']:
                jobsManager.add_finished_job(job)


def time_to_epoch(time):
    dt = datetime.strptime(time, '%Y-%m-%dT%H:%M:%S')
    unix_epoch_time = int(dt.timestamp())
    return unix_epoch_time


def random_usage():
    # Generate a random number between 1 and 10
    return random.uniform(1, 100)


def save_metrics(job, host, metric):
    # print(job.job_id, metric)
    job_cpu_gauge.labels(jobid=job.job_id, user=job.user, jobname=job.job_name, hostname=host).set(metric['cpu'])
    job_mem_gauge.labels(jobid=job.job_id, user=job.user, jobname=job.job_name, hostname=host).set(metric['mem'])
    job_date_submit.labels(jobid=job.job_id, user=job.user, jobname=job.job_name, hostname=host).set(time_to_epoch(job.date_submit))
    job_date_start.labels(jobid=job.job_id, user=job.user, jobname=job.job_name, hostname=host).set(time_to_epoch(job.date_start))


# def update_finished_job(job_id):
#     job = jobsManager.find_job_by_id(job_id)
#     if job.date_end == 'Unknown':
#         now = datetime.now()
#         job.date_end = now.strftime('%Y-%m-%dT%H:%M:%S')
#     job_date_end.labels(jobid=job.job_id, user=job.user, jobname=job.job_name, hostname=job.host).set(time_to_epoch(job.date_end))
#     _print("Job finished:", job)
#     jobsManager.mark_job_completed(job)

@app.route("/get_pending_jobs", methods=['GET'])
async def get_pending_jobs():
    with app.app_context():
#        print(jobsManager.get_json())
        return jsonify(jobsManager.to_json(jobsManager.get_pending_jobs())), 200

@app.route("/get_running_jobs", methods=['GET'])
async def get_running_jobs():
    with app.app_context():
#        print(jobsManager.get_json())
        return jsonify(jobsManager.to_json(jobsManager.get_running_jobs())), 200

@app.route("/get_finished_jobs", methods=['GET'])
async def get_finished_jobs():
    with app.app_context():
#        print(jobsManager.get_json())
        return jsonify(jobsManager.to_json(jobsManager.get_finished_jobs())), 200

def run_flask():
    with app.app_context():
        app.run(debug=True, use_reloader=False, threaded=True)
        #app.run()

def main():
        # Start the Prometheus HTTP server
        start_http_server(7999)
        # Start new thread for Flask
        x = threading.Thread(target=run_flask)
        x.daemon = True
        x.start()
        
        while True:
            try:
                start_t = time.time()
                read_jobs()
                running_hosts = jobsManager.get_running_jobs_host()
                if len(running_hosts) > 0:
                    metrics = read_metrics(running_hosts)
                    for job in jobsManager.get_running_jobs():
                        print(job)
                        if '[' in job.host:
                            hosts = parse_hosts([job.host])
#                            print(hosts)
                            for i in hosts:
                                save_metrics(job, i, metrics[i])
                        else:
                            save_metrics(job, job.host, metrics[job.host])
                        #print(job)
                end_t = time.time()
                my_time = end_t-start_t
                if my_time > 5:
                    _print("Time >5 secs:", my_time)
                #_print("Active jobs: ", jobsManager.get_running_hosts())
                time.sleep(10)
            except Exception as e:
            #print("Error: ", e)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                _print(exc_type, fname, exc_tb.tb_lineno)
                _print(traceback.format_exc())

if __name__ == '__main__':
    main()


