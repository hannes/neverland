#!/bin/sh
for i in /home/hannes/neverland/ssbm/queries/*.sql
do
  echo $i
  killall mserver5
  sleep 10
  sudo bash -c "echo 3 > /proc/sys/vm/drop_caches"
  /home/hannes/neverland/monetdb-install/bin/mserver5 --dbpath=/home/hannes/neverland/ssbm/SF-100/ssbm-sf100 --set monet_vault_key=/home/hannes/neverland/ssbm/SF-100/ssbm-sf100/.vaultkey --set gdk_vmtrim=NOOOOO --daemon=yes --set mapi_port=50000 --set mapi_open=true &
  sudo stap --skip-badvars /home/hannes/neverland/mmap.stp > /home/hannes/neverland/stapres/`basename $i`.csv  &
  sleep 20
  /home/hannes/neverland/monetdb-install/bin/mclient $i
  sleep 10
  sudo killall stap
done
