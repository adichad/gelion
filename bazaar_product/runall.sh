#!/bin/bash

exec > /dev/null
exec 2>&1

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd -P`
popd > /dev/null

echo $SCRIPTPATH

nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i  0 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i  1 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i  2 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i  3 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i  4 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i  5 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i  6 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i  7 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i  8 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i  9 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i 10 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i 11 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i 12 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i 13 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i 14 &
nohup python $SCRIPTPATH/batch_push.py -t 4 -b 100 -p 16 -i 15 &

