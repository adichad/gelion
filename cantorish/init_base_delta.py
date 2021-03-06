
from config import Config
import json
from tools import MySQLDB, Encoder, CantorishInitBaseDeltaUpdater
from queries import queryMap
import logging
import logging.handlers
import os,sys
import getopt

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

logger = logging.getLogger('etl_cantorish')
logger.setLevel(logging.INFO)
LOG_FILENAME = "/tmp/etl.cantorish_init_base.%s.delta.log"%env
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=100000000, backupCount=5)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

pid = str(os.getpid())
pidfile = "/tmp/etl.cantorish_init_base.%s.delta.pid"%env

if os.path.isfile(pidfile):
  logger.warn( "%s already exists, exiting" % pidfile )
  sys.exit()

file(pidfile, 'w').write(pid)

try:
  cfg = Config(file(config_file))[env]
  db_source = MySQLDB(cfg['db']['source'])
  db_target = MySQLDB(cfg['db']['management'])
  deltaUpdater = CantorishInitBaseDeltaUpdater(db_source, db_target, queryMap, range)
  deltaUpdater.streamDelta(batch_size)
  
  db_source.close()
  db_target.close()
except:
  logger.error("exception: %s"%str(sys.exc_info()))
  raise 
finally:
  os.unlink(pidfile)

