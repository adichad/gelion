
from config import Config
import json
from tools import BaseDB, ProductsShaper, MandelbrotPipe
from queries import queryMap
import grequests
import getopt, sys

env = 'default'
config_file = 'simple.cfg'
start_id = 0
end_id = sys.maxint
threads = 4
batch_size = 10

opts, args = getopt.getopt(sys.argv[1:], 'e:', ['env='])

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



cfg = Config(file(config_file))[env]

db_source = BaseDB(cfg['db']['source'])
db_target = BaseDB(cfg['db']['management'])
url = cfg['mandelbrot']['url']
shaper = ProductsShaper(db_source, queryMap)
pipe = MandelbrotPipe(db_source, db_target, queryMap, start_id, end_id, shaper, url, grequests.Pool(threads))
pipe.streamDelta(batch_size)


db_source.close()
db_target.close()

