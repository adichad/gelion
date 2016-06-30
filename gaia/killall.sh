#!/bin/bash
kill $(echo `ps ax|grep gaia|grep batch_push|cut -c1-6`)
