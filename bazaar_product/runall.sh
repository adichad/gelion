#!/bin/bash

procs=16
threads=1
batch_size=100
env=default


while getopts ":e:b:t:p:" opt; do
  case $opt in
    e)
      env=$OPTARG
      ;;
    b)
      batch_size=$OPTARG
      ;;
    t)
      threads=$OPTARG
      ;;
    p)
      procs=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

exec > /dev/null
exec 2>&1

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd -P`
popd > /dev/null

echo $SCRIPTPATH

for (( i=0; i<$procs; i++ ))
do
  nohup python $SCRIPTPATH/batch_push.py -e $env -t $threads -b $batch_size -p $procs -i $i &
done

