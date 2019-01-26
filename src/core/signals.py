# -*- coding: utf-8 -*-

import django_rq
import datetime
from job import cron_job

def syncdb_callback_function(sender, instance, **kwargs):
    if sender._meta.db_table == "core_user":
        queue = django_rq.get_queue('high')
        queue.enqueue(cron_job, "delcache/", {"id": instance.pk})
