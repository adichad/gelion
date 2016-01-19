#!/bin/bash

time python batch_push.py -t 4 -b 100 -f 0 -l 100000 &
time python batch_push.py -t 4 -b 100 -f 100000 -l 200000 &
time python batch_push.py -t 4 -b 100 -f 200000 -l 300000 &
time python batch_push.py -t 4 -b 100 -f 300000 -l 400000 &
time python batch_push.py -t 4 -b 100 -f 400000 -l 500000 &
time python batch_push.py -t 4 -b 100 -f 500000 -l 600000 &
time python batch_push.py -t 4 -b 100 -f 600000 -l 700000 &
time python batch_push.py -t 4 -b 100 -f 700000 -l 800000 &

