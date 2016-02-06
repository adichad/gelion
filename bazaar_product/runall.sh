#!/bin/bash

procs=16
threads=1
batch_size=100

exec > /dev/null
exec 2>&1

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd -P`
popd > /dev/null

echo $SCRIPTPATH

for (( i=0; i<$procs; i++ ))
do
  nohup python $SCRIPTPATH/batch_push.py -t $threads -b $batch_size -p $procs -i $i &
done

