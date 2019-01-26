import requests
import django_rq
from core import job

queue = django_rq.get_queue('high')
queue.enqueue(job.cron_job, "cron/worship/", {})