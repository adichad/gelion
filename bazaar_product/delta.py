
from config import Config
import json
from tools import BaseDB, Encoder, ProductDeltaUpdater
from queries import queryMap
import logging
import logging.handlers
import os,sys
import getopt

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

script_path = os.path.dirname(os.path.abspath(__file__))
env = 'default'
config_file = script_path+'/simple.cfg'
range = 64
batch_size = 1000

opts, args = getopt.getopt(sys.argv[1:], 'e:c:i:p:t:b:', ['env=', 'conf=', 'batch_size=', 'range='])

for k, v in opts:
  if k in ("-e", "--env"):
    env = v
  elif k in ("-c", "--conf"):
    config_file = v
  elif k in ("-b", "-batch_size"):
    batch_size = int(v)
  elif k in ("-r", "-range"):
    range = int(v)


try:
  cfg = Config(file(config_file))[env]
  db_source = BaseDB(cfg['db']['source'])
  db_target = BaseDB(cfg['db']['management'])

  deltaUpdater = ProductDeltaUpdater(db_source, db_target, queryMap, range)
  deltaUpdater.streamDelta(batch_size)
  db_source.close()
  db_target.close()
except:
  logger.error("exception: %s"%str(sys.exc_info()))
  raise 
finally:
  os.unlink(pidfile)

