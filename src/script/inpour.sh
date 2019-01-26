#!/bin/bash

. /home/ubuntu/srv/PtKingdom/bin/activate
export PYTHONPATH=$PYTHONPATH:/home/ubuntu/srv/PtKingdom/src
export DJANGO_SETTINGS_MODULE=back.settings
python /home/ubuntu/srv/PtKingdom/src/script/inpour.py