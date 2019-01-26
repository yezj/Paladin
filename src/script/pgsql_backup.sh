#!/bin/bash
export PGPASSWORD='4wxto.kNAE.cujML97'
/usr/bin/pg_dump -h 127.0.0.1 -f /home/ubuntu/srv/PtKingdom/backup/`date +%Y%m%d%H%M`_ptkingdom.tar -U deploy -F t ptkingdom
