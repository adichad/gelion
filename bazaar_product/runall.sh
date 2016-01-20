#!/bin/bash

exec > /dev/null
exec 2>&1

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd -P`
popd > /dev/null

echo $SCRIPTPATH

nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f       0 -l  100000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f  100000 -l  200000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f  200000 -l  300000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f  300000 -l  400000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f  400000 -l  500000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f  500000 -l  600000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f  600000 -l  700000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f  700000 -l  800000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f  800000 -l  900000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f  900000 -l 1000000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f 1000000 -l 1100000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f 1100000 -l 1200000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f 1200000 -l 1300000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f 1300000 -l 1400000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f 1400000 -l 1500000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f 1500000 -l 1600000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f 1600000 -l 1700000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f 1700000 -l 1800000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f 1800000 -l 1900000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f 1900000 -l 2000000 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -f 2000000 -l 2100000 &

