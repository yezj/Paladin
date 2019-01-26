#!/bin/bash
# . /home/deploy/srv/PtKingdom/bin/activate
# export PYTHONPATH=$PYTHONPATH:/home/deploy/srv/PtKingdom/src
# export DJANGO_SETTINGS_MODULE=back.settings
# python /home/deploy/srv/PtKingdom/src/script/store_job.py

. /home/ubuntu/srv/PtKingdom/bin/activate
export PYTHONPATH=$PYTHONPATH:/home/ubuntu/srv/PtKingdom/src
export DJANGO_SETTINGS_MODULE=back.settings
python /home/ubuntu/srv/PtKingdom/src/script/store_job.py