from flask import Flask, Response, request
import json
from config import Config
from tools import MSSQLDB, MySQLDB, Encoder, GroceryShaper
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

logger = logging.getLogger('reindex_grocery')
logger.setLevel(logging.INFO)
LOG_FILENAME = "/tmp/reindex.grocery.%s.log"%env
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=100000000, backupCount=5)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

pid = str(os.getpid())
pidfile = "/tmp/reindex.grocery.%s.pid"%env

if os.path.isfile(pidfile):
  logger.warn( "%s already exists, exiting" % pidfile )
  sys.exit()

file(pidfile, 'w').write(pid)
cfg = Config(file(config_file))[env]
db_source = MSSQLDB(cfg['db']['source'])
db_target = MySQLDB(cfg['db']['management'])
db_orders = MySQLDB(cfg['db']['orders'])
url = cfg['mandelbrot']['url']

try:
  logger.info("initiating app")
  app = Flask(__name__)
  logger.info("initiated app")

  shaper = GroceryShaper(db_source, db_orders, queryMap)

  @app.route('/reindex/grocery/<int:variant_id>', methods=['POST', 'GET'])
  def reindex_grocery(variant_id):
    try:
      logger.info("received: %s"%variant_id)
      result = db_target.get("select variant_id, source_log_id, target_log_id, timestamp, bucket, last_error from grocery_status where variant_id=%s limit 1"%variant_id)
      if len(result)>0:
        db_target.put("update grocery_status set target_log_id = 0 where variant_id=%s limit 1"%variant_id)
        rv = {"variant_id": variant_id, "current-shape": shaper.shape([variant_id]), "previous-status": result[0], "reindex-scheduled": True, "mandelbrot-url": "http://mandelbrot-30.production.askmebazaar.com:9999/get/grocery/%s"%variant_id}
      else:
        rv = {"variant_id": variant_id, "reindexed": False, "reason": "not found"}
    except:
      rv = str(sys.exc_info())
      logger.error("exception: %s"%rv)
      rv = { "exception": rv }

    return Response(json.dumps(rv, cls=Encoder, indent=2), mimetype='application/json')


  if __name__ == '__main__':
    
    logger.info("running app")
    app.run(host='0.0.0.0', port=9998)
    logger.info("app running")

except:
  logger.error("exception: %s"%str(sys.exc_info()))
  raise 
finally:
  logger.info("exiting app")
  os.unlink(pidfile)
  db_target.close()


