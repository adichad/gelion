#!/bin/bash
kill $(echo `ps ax|grep cantorish|grep batch_push|cut -c1-6`)
