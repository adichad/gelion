
from config import Config
import json
from tools import BaseDB, ProductsShaper, MandelbrotPipe
from queries import queryMap
import grequests
import getopt, sys
import os

import signal
import time
import logging
import logging.handlers

logger = logging.getLogger('etl_product')
logger.setLevel(logging.INFO)


class GracefulKiller:
  runMore = True
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, signum, frame):
    self.runMore = False


if __name__ == '__main__':
  killer = GracefulKiller()
  script_path = os.path.dirname(os.path.abspath(__file__))

  env = 'default'
  config_file = script_path+'/simple.cfg'
  start_id = 0
  end_id = sys.maxint
  threads = 4
  batch_size = 10

  opts, args = getopt.getopt(sys.argv[1:], 'e:c:f:l:t:b:', ['env=', 'conf=', 'first=', 'last=', 'threads=', 'batch-size='])

  for k, v in opts:
    if k in ("-e", "--env"): 
      env = v
    elif k in ("-c", "--conf"):
      config_file = v
    elif k in ("-f", "--first"):
      start_id = int(v)
    elif k in ("-l", "--last"):
      end_id = int(v)
    elif k in ("-t", "--threads"):
      threads = int(v)
    elif k in ("-b", "--batch-size"):
      batch_size = int(v)

  pid = str(os.getpid())
  pidfile = "/tmp/etl.product.%(env)s.%(start_id)i-%(end_id)i.pid"%locals()

  LOG_FILENAME = "/tmp/etl.product.%(env)s.%(start_id)i-%(end_id)i.log"%locals()
  handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=100000000, backupCount=5)
  handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
  logger.addHandler(handler)

  if os.path.isfile(pidfile):
    logger.warn( "%s already exists, exiting" % pidfile )
    sys.exit()

  file(pidfile, 'w').write(pid)

  try:
    logger.info("pidfile: "+pidfile)
    cfg = Config(file(config_file))[env]

    db_source = BaseDB(cfg['db']['source'])
    db_target = BaseDB(cfg['db']['management'])
    url = cfg['mandelbrot']['url']
    shaper = ProductsShaper(db_source, queryMap)
    pipe = MandelbrotPipe(db_source, db_target, queryMap, start_id, end_id, shaper, url, grequests.Pool(threads))
    while killer.runMore:
      pipe.streamDelta(batch_size)
      time.sleep(10)

  finally:
    os.unlink(pidfile)
    db_source.close()
    db_target.close()
    

