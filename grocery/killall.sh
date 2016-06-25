#!/bin/bash
kill $(echo `ps ax|grep grocery|grep batch_push|cut -c1-6`)
