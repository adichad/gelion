
import json
import decimal
import datetime
import MySQLdb
import MySQLdb.cursors
import pymssql
import psycopg2
import psycopg2.extras
from config import Config
from itertools import groupby
import grequests
import logging
from queries import queryMap
from random import randint
import logging
import time
import statistics
from decimal import Decimal

logger = logging.getLogger('etl_grocery')

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
    except (AttributeError, MySQLdb.OperationalError):
      self.connect()
      cur = self.conn.cursor()
      cur.execute(query, params)
    return cur

  def close(self):
    self.conn.close()


class MSSQLDB(object):
  conn = None
  config = None

  def __init__(self, config):
    self.config = config
    self.connect()

  def connect(self):
    self.conn = self.getConn()

  def getConn(self):
    return pymssql.connect(
      server = self.config.host,
      port = self.config.port,
      user = self.config.user,
      password = self.config.passwd,
      database = self.config.name)

  def putIdempotent(self, query, params=()):
    try:
      cur = self.conn.cursor(as_dict=True)
      cur.execute(query, params)
      self.conn.commit()
    except (AttributeError, pymssql.OperationalError):
      self.connect()
      cur = self.conn.cursor(as_dict=True)
      cur.execute(query, params)
      self.conn.commit()
    
    insid = cur.lastrowid
    cur.close()
    return insid

  def put(self, query, params=()):
    cur = self.conn.cursor(as_dict=True)
    cur.execute(query, params)
    self.conn.commit()
    insid = cur.lastrowid
    cur.close()
    return insid

  def get(self, query, params=()):
    try:
      cur = self.conn.cursor(as_dict=True)
      cur.execute(query, params)
    except (AttributeError, pymssql.OperationalError):
      if self.conn is not None:
        try:
          self.conn.close()
        except:
          None
      self.connect()
      cur = self.conn.cursor(as_dict=True)
      cur.execute(query, params)

    result = list(cur.fetchall())
    cur.close()
    return result

  def getCursor(self, query, params=()):
    cur = None
    try:
      conn = self.getConn()
      cur = conn.cursor(as_dict=True)
      cur.execute(query, params)
    except (AttributeError, pymssql.OperationalError):
      conn = self.getConn()
      cur = conn.cursor(as_dict=True)
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


class GroceryShaper(object):
  db = None
  queryMap = None

  def __init__(self, db, queryMap):
    self.db = db
    self.queryMap = queryMap

  def shape(self, ids = []):
    format_strings = ','.join(['%s'] * len(ids))
    groceries = self.db.get(self.queryMap["grocery_variant"] % (format_strings), tuple(ids))
    for grocery in groceries:
      grocery['media'] = self.db.get(self.queryMap["grocery_media"] % tuple([str(grocery['variant_id'])]))
      grocery['categories'] = self.db.get(self.queryMap["grocery_category"] % tuple([str(grocery['product_id'])]))
      grocery['category_hierarchy'] = self.db.get(self.queryMap["grocery_category_hierarchy"] % tuple([str(grocery['product_id']), str(grocery['brand_id'])]))
      for level in [2, 3, 4, 5]:
        for category in filter(lambda c: c['level'] == level, grocery['category_hierarchy']):
          parent_category = filter(lambda c: c['id'] == category['parent_id'], grocery['category_hierarchy'])
          parent_boost = float(parent_category[0]['category_brand_priority'])/2.0 if len(parent_category) > 0 else 0.0
          category['category_brand_priority'] = float(category['category_brand_priority']) + parent_boost

      grocery['category_l1'] = filter(lambda c: c['level'] == 1 and c['isnav'] == 1, grocery['category_hierarchy'])
      grocery['category_l2'] = filter(lambda c: c['level'] == 2 and c['isnav'] == 1, grocery['category_hierarchy'])
      grocery['category_l3'] = filter(lambda c: c['level'] == 3 and c['isnav'] == 1, grocery['category_hierarchy'])
      grocery['category_l4'] = filter(lambda c: c['level'] == 4 and c['isnav'] == 1, grocery['category_hierarchy'])
      grocery['category_l5'] = filter(lambda c: c['level'] == 5 and c['isnav'] == 1, grocery['category_hierarchy'])
      grocery['variant_title_head'] = filter(lambda y: len(y)>0, map(lambda x: x.strip(), grocery['variant_title'].split("+", 1)))
      grocery['variant_title_head'] = grocery['variant_title_head'][0]  # if len(grocery['variant_title_head']) == 1 else ''
      grocery['items'] = self.db.get(self.queryMap["grocery_item"] % tuple([str(grocery['variant_id'])]))
      grocery['default_customer_price'] = statistics.median(map(lambda i: i['customer_price'], grocery['items'])) if len(grocery['items']) > 0 else 0.0
      grocery['default_display_price'] = max(map(lambda i: i['display_price'], grocery['items'])) if len(grocery['items']) > 0 else 0.0
      grocery['default_discount_percent'] = float(grocery['default_display_price'] - grocery['default_customer_price'])*100.0/float(grocery['default_display_price']) if grocery['default_display_price'] > 0 else 0.0
      for item in grocery['items']:
        item['postal_codes'] = filter(lambda p: p !="", map(lambda p: p.strip(), (item['postal_codes'] or "").split(';')))
        item['areas'] = map(lambda a: int(a), filter(lambda p: p !="", map(lambda p: p.strip(), (item['areas'] or "").split(','))))
        item['delivery_days'] = map(lambda a: int(a), filter(lambda p: p !="", map(lambda p: p.strip(), (item['delivery_days'] or "").split(','))))
        item['delivery_days'] = item['delivery_days'][0] if len(item['delivery_days']) > 0 else 0
        item['storefronts'] = self.db.get(self.queryMap["grocery_storefront"] % tuple([str(item['id'])]))
    # logger.info("groceries: "+json.dumps(groceries, cls=Encoder, indent=2)) 
    return groceries



class GroceryDeltaUpdater(object):
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
    cur = self.db_source.getCursor(self.queries["grocery_delta_fetch"], (idPrev, idCurr, ))
    deltaBatch = cur.fetchmany(batchSize)
    while len(deltaBatch)>0:
      format_strings = ','.join(['%s'] * len(deltaBatch))
      self.db_target.put(
        self.queries["grocery_delta_merge"] % format_strings % 
        tuple(map(lambda rec: "(%s, '%s', %s)"%(str(rec['variant_id']), rec['last_log_id'], str(randint(0, self.procs-1))), deltaBatch))
      )
      count+=len(deltaBatch)
      deltaBatch = cur.fetchmany(batchSize)

    cur.connection.close()
    cur.close()
    end = int(round(time.time() * 1000))
    self.db_target.put(self.queries["grocery_bookmark_insert"], (idCurr, count, (end-start)))

  def idPrev(self): 
    result = self.db_target.get(self.queries["last_target_log_id"])
    return result[0]['log_id'] if len(result) > 0 else 0

  def idCurr(self):
    result = self.db_source.get(self.queries["max_source_log_id"])
    return result[0]['max_id'] if len(result) > 0 else 0



def hook_factory(*factory_args, **factory_kwargs):
  def updateStatus(result, *args, **kwargs):
    logger.info("target response: "+result.text)
    successes = []
    failures = []
    try:
      res = json.loads(result.text)
      if isinstance(res, dict) and ('response' in res) and ('successful' in res['response']):
        successes = filter(lambda rec: rec['variant_id'] in map(lambda id: long(id), res['response']['successful']), factory_kwargs['delta_batch'])
        
        success_ids = map(lambda rec: rec['variant_id'], successes)
        failed_list = filter(lambda rec: rec['variant_id'] not in success_ids, factory_kwargs['delta_batch'])
        tgt_errors = dict((long(rec['variant_id']), rec['error']) for rec in res['response']['failed'])
        failures = map(lambda rec: {"variant_id": long(rec['variant_id']), "source_log_id": rec['source_log_id'], "error": tgt_errors[long(rec['variant_id'])] if long(rec['variant_id']) in tgt_errors else "no such variant_id in source"}, failed_list)
      else:
        failures = map(lambda rec: {"variant_id": rec['variant_id'], "source_log_id": rec['source_log_id'], "error": result.text}, factory_kwargs['delta_batch'])
    except Exception, e:
      logger.error("Exception: "+str(e))
      successes = []
      failures = map(lambda rec: {"variant_id": rec['variant_id'], "source_log_id": rec['source_log_id'], "error": result.text}, factory_kwargs['delta_batch'])
 
    db = factory_kwargs['db']
    try:
      if len(successes) > 0:
        format_strings = ','.join(['%s'] * len(successes))
        db.put(queryMap["grocery_success_merge"] % format_strings % tuple(map(lambda rec: "(%s, %s, %s)"%(str(rec['variant_id']), str(rec['source_log_id']), str(rec['source_log_id'])), successes)))
      if len(failures) > 0:
        format_strings = ','.join(['%s'] * len(failures))
        db.put(queryMap["grocery_failure_merge"] % format_strings % tuple(map(lambda rec: "(%s, %s, '%s')"%(str(rec['variant_id']), str(rec['source_log_id']), str(rec['error'])), failures)))
    except Exception, e:
      logger.error(str(e))
    result.close()
    return None
  return updateStatus


class MandelbrotPipe(object):
  db_source = None
  db_target = None
  queries = None
  proc_id = None
  procs = None
  shaper = None
  url = None
  requestPool = None

  def __init__(self, db_source, db_target, queries, proc_id, procs, shaper, url, requestPool):
    self.db_source = db_source
    self.db_target = db_target
    self.queries = queries
    self.proc_id = proc_id
    self.procs = procs
    self.shaper = shaper
    self.url = url
    self.requestPool = requestPool

  
  def post(self, data, deltaBatch):
    try:
      logger.info(self.url)
      req = grequests.post(self.url, data = json.dumps(data, cls=Encoder, indent=2), hooks = {'response': [hook_factory(delta_batch=deltaBatch, db=self.db_target)]})
      return grequests.send(req, self.requestPool)
    except Exception, e:
      failures = map(lambda rec: {"variant_id": rec['variant_id'], "source_log_id": rec['source_log_id'], "error": str(e)}, deltaBatch)
      format_strings = ','.join(['%s'] * len(failures))
      self.db_target.put(queryMap["grocery_failure_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s', '%s')"%(str(rec['variant_id']), rec['source_log_id'], str(rec['error'])), failures)))
      return None

  def streamDelta(self, batchSize, killer):
    start = int(round(time.time() * 1000))
    cur = self.db_target.getCursor(self.queries["grocery_id_fetch"], (self.proc_id, self.procs, ))
    deltaBatch = cur.fetchmany(batchSize)
    jobs = []
    count = 0
    while len(deltaBatch)>0 and killer.runMore:
      ids = map(lambda rec: rec['variant_id'], deltaBatch)
      logger.info("shaping variant_ids: "+json.dumps(ids, cls=Encoder))
      shaped = self.shaper.shape(ids)
      job = self.post(shaped, deltaBatch)
      if job is not None:
        jobs.append(job)
      count+=len(deltaBatch)
      deltaBatch = cur.fetchmany(batchSize)
    cur.close()
    map(lambda job: job.join(), jobs)
    end = int(round(time.time() * 1000))
    logger.info("batch [%s mod %s]%s record count: %s; processing time (ms): %s"%(str(self.proc_id), str(self.procs), "" if killer.runMore else " [killed]", str(count), str(end - start)))


