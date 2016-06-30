#!/bin/bash
kill $(echo `ps ax|grep bazaar_product|grep batch_push|cut -c1-6`)
