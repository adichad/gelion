
import json
import decimal
import datetime
import MySQLdb
import MySQLdb.cursors
import psycopg2
import psycopg2.extras
from config import Config
from itertools import groupby
import grequests
import logging
from random import randint
import logging
import time

logger = logging.getLogger('etl_product')

class Encoder(json.JSONEncoder):
  def default(self, o):
    if isinstance(o, decimal.Decimal):
      return float(o)
    if isinstance(o, datetime.date):
      return o.strftime("%Y-%m-%d %H:%M:%S")
    return super(Encoder, self).default(o)


class MySQLDB(object):
  conn = None
  config = None

  def __init__(self, config):
    self.config = config
    self.connect()

  def connect(self):
    self.conn = self.getConn()

  def getConn(self):
    return MySQLdb.connect(
      host = self.config.host,
      port = self.config.port,
      user = self.config.user,
      passwd = self.config.passwd,
      db = self.config.name,
      charset = 'utf8',
      init_command = "SET SESSION wait_timeout=3000;SET NAMES UTF8;SET time_zone='+0:00';SET SESSION group_concat_max_len = 1000000; SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
      cursorclass = MySQLdb.cursors.DictCursor)

  def putIdempotent(self, query, params=()):
    try:
      cur = self.conn.cursor()
      cur.execute(query, params)
      self.conn.commit()
    except (AttributeError, MySQLdb.OperationalError):
      self.connect()
      cur = self.conn.cursor()
      cur.execute(query, params)
      self.conn.commit()
    
    insid = cur.lastrowid
    cur.close()
    return insid

  def put(self, query, params=()):
    cur = self.conn.cursor()
    cur.execute(query, params)
    self.conn.commit()
    insid = cur.lastrowid
    cur.close()
    return insid

  def get(self, query, params=()):
    try:
      cur = self.conn.cursor()
      cur.execute(query, params)
    except (AttributeError, MySQLdb.OperationalError):
      self.connect()
      cur = self.conn.cursor()
      cur.execute(query, params)

    result = list(cur.fetchall())
    cur.close()
    return result

  def getCursor(self, query, params=()):
    cur = None
    try:
      cur = self.conn.cursor()
      cur.execute(query, params)
    except (AttributeError, psycopg2.OperationalError):
      self.connect()
      cur = self.conn.cursor()
      cur.execute(query, params)
    return cur

  def close(self):
    self.conn.close()


class PostgreSQLDB(object):
  conn = None
  config = None

  def __init__(self, config):
    self.config = config
    self.connect()

  def connect(self):
    self.conn = self.getConn()

  def getConn(self):
    return psycopg2.connect(
      host = self.config.host,
      port = self.config.port,
      user = self.config.user,
      password = self.config.passwd,
      database = self.config.name,
      cursor_factory = psycopg2.extras.RealDictCursor)

  def putIdempotent(self, query, params=()):
    try:
      cur = self.conn.cursor()
      cur.execute(query, params)
      self.conn.commit()
    except (AttributeError, psycopg2.OperationalError):
      self.connect()
      cur = self.conn.cursor()
      cur.execute(query, params)
      self.conn.commit()
    
    insid = cur.lastrowid
    cur.close()
    return insid

  def put(self, query, params=()):
    cur = self.conn.cursor()
    cur.execute(query, params)
    self.conn.commit()
    insid = cur.lastrowid
    cur.close()
    return insid

  def get(self, query, params=()):
    try:
      cur = self.conn.cursor()
      cur.execute(query, params)
    except (AttributeError, psycopg2.OperationalError):
      self.connect()
      cur = self.conn.cursor()
      cur.execute(query, params)

    result = list(cur.fetchall())
    cur.close()
    return result

  def getCursor(self, query, params=()):
    cur = None
    try:
      cur = self.conn.cursor()
      cur.execute(query, params)
    except (AttributeError, psycopg2.OperationalError):
      self.connect()
      cur = self.conn.cursor()
      cur.execute(query, params)
    return cur

  def close(self):
    self.conn.close()

class GeoDeltaUpdater(object):
  db_source = None
  db_target = None
  queries = None
  procs = None

  def __init__(self, db_source, db_target, queries, procs):
    self.db_source = db_source
    self.db_target = db_target
    self.queries = queries
    self.procs = procs

  def streamDelta(self, batchSize):
    start = int(round(time.time() * 1000))
    idPrev = self.idPrev()
    idCurr = self.idCurr()
    logger.info("last processed log_id: "+str(idPrev))
    logger.info("current max    log_id: "+str(idCurr))
    count = 0
    cur = self.db_source.getCursor(self.queries["geo_delta_fetch"], (idPrev, idCurr, ))
    deltaBatch = cur.fetchmany(batchSize)
    while len(deltaBatch)>0:
      format_strings = ','.join(['%s'] * len(deltaBatch))
      self.db_target.put(
        self.queries["geo_delta_merge"] % format_strings %
        tuple(map(lambda rec: "(%s, '%s', %s)"%(str(rec['gid']), str(rec['last_updated_dt'])[:-6], str(randint(0, self.procs-1))), deltaBatch))
      )
      count+=len(deltaBatch)
      deltaBatch = cur.fetchmany(batchSize)

    cur.close()
    end = int(round(time.time() * 1000))
    self.db_target.put(self.queries["geo_bookmark_insert"], (idCurr, count, (end-start)))

  def idPrev(self):
    result = self.db_target.get(self.queries["last_target_log_id"])
    return result[0]['log_id'] if len(result) > 0 else 0

  def idCurr(self):
    result = self.db_source.get(self.queries["max_source_log_id"])
    return result[0]['max_id'] if len(result) > 0 else 0






cfg = Config(file('simple.cfg'))['default']

db_source = PostgreSQLDB(cfg['db']['source'])

res = db_source.get("select * from geoinfo limit 1")

db_source.close()
print(res)
print(json.dumps(res, cls=Encoder, indent=2))
print("hellow")



