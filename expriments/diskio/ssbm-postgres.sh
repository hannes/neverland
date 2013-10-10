#!/bin/sh
for i in /home/hannes/neverland/ssbm/queries/*.sql
do
  echo $i
  killall postgres
  sleep 10
  sudo bash -c "echo 3 > /proc/sys/vm/drop_caches"
  /home/hannes/neverland/postgres-install/bin/postgres -D /home/hannes/neverland/ssbm/pgdata-sf100 &
  sudo stap --skip-badvars /home/hannes/neverland/mmap.stp > /home/hannes/neverland/stapres/`basename $i`.csv  &
  sleep 20
  /home/hannes/neverland/postgres-install/bin/psql ssbm-sf100 -f $i > /dev/null
  sleep 10
  sudo killall stap
done
