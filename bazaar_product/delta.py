
from config import Config
import json
from tools import BaseDB, Encoder, ProductDeltaUpdater
from queries import queryMap
import logging
import logging.handlers
import os,sys

logger = logging.getLogger('etl_product')
logger.setLevel(logging.INFO)
LOG_FILENAME = "/tmp/etl.product.delta.log"
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=100000000, backupCount=5)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

pid = str(os.getpid())
pidfile = "/tmp/etl.product.delta.pid"
if os.path.isfile(pidfile):
  logger.warn( "%s already exists, exiting" % pidfile )
  sys.exit()

file(pidfile, 'w').write(pid)

try:
  script_path = os.path.dirname(os.path.abspath(__file__))
  cfg = Config(file(script_path+'/simple.cfg'))

  db_source = BaseDB(cfg['default']['db']['source'])
  db_target = BaseDB(cfg['default']['db']['management'])

  deltaUpdater = ProductDeltaUpdater(db_source, db_target, queryMap)
  deltaUpdater.streamDelta(1000)
finally:
  os.unlink(pidfile)
  db_source.close()
  db_target.close()

