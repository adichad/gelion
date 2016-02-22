
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
from queries import queryMap
from random import randint
import logging
import time

logger = logging.getLogger('etl_geo')

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


class GeoShaper(object):
  db = None
  queryMap = None

  def __init__(self, db, queryMap):
    self.db = db
    self.queryMap = queryMap

  def ancestorIDList(self, ids, accumulator, level):
    if(ids is not None and len(ids)>0 and level < 2):
      accumulator.extend(ids)
      format_strings = ','.join(['%s'] * len(ids))
      containers = filter(lambda c: c is not None and len(c)>0, map(lambda c: c['containers'], self.db.get(self.queryMap["container_ids"] % format_strings, tuple(ids))))
      containers = [i for s in containers for i in s]
      self.ancestorIDList(containers, accumulator, level+1)

  def containers(self, ids = []):
    accumulator = []
    self.ancestorIDList(ids, accumulator, 0)
    format_strings = ','.join(['%s'] * len(accumulator))
    return self.db.get(self.queryMap["containers_dag"] % (format_strings), tuple(accumulator)) if len(accumulator) else []


  def shape(self, ids = []):
    format_strings = ','.join(['%s'] * len(ids))
    logger.debug(self.queryMap["base_geo"] % (format_strings)% tuple(ids))
    geos = self.db.get(self.queryMap["base_geo"] % (format_strings), tuple(ids))
    for geo in geos:
      geo['containers_dag'] = self.containers(geo['containers'])
      geo['related'] = [] if geo['related'] is None else geo['related']
      format_strings = ','.join(['%s'] * len(geo['related']))
      geo['related_list'] = self.db.get(self.queryMap["related_list"] % (format_strings), tuple(geo['related'])) if len(geo['related']) else []
    
    #print("geos: "+json.dumps(geos, cls=Encoder, indent=2)) 
    return geos



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



def hook_factory(*factory_args, **factory_kwargs):
  def updateStatus(result, *args, **kwargs):
    logger.info("target response: "+result.text)
    successes = []
    failures = []
    try:
      res = json.loads(result.text)
      if isinstance(res, dict):
        successes = filter(lambda rec: rec['gid'] in map(lambda id: long(id), res['response']['successful']), factory_kwargs['delta_batch'])
        success_ids = map(lambda rec: rec['gid'], successes)
        failed_list = filter(lambda rec: rec['gid'] not in success_ids, factory_kwargs['delta_batch'])
        tgt_errors = dict((long(rec['gid']), rec['error']) for rec in res['response']['failed'])
        failures = map(lambda rec: {"gid": long(rec['gid']), "source_dt": rec['source_dt'], "error": tgt_errors[long(rec['gid'])] if long(rec['gid']) in tgt_errors else "no such gid in source"}, failed_list)
      else:
        failures = map(lambda rec: {"gid": rec['gid'], "source_dt": rec['source_dt'], "error": result.text}, factory_kwargs['delta_batch'])
    except Exception, e:
      logger.error("Exception: "+str(e))
      successes = []
      failures = map(lambda rec: {"gid": rec['gid'], "source_dt": rec['source_dt'], "error": result.text}, factory_kwargs['delta_batch'])
 
    db = factory_kwargs['db']
    try:
      if len(successes) > 0:
        format_strings = ','.join(['%s'] * len(successes))
        db.put(queryMap["geo_success_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s', '%s')"%(str(rec['gid']), str(rec['source_dt']), str(rec['source_dt'])), successes)))
      if len(failures) > 0:
        format_strings = ','.join(['%s'] * len(failures))
        db.put(queryMap["geo_failure_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s', '%s')"%(str(rec['gid']), str(rec['source_dt']), str(rec['error'])), failures)))
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
      failures = map(lambda rec: {"gid": rec['gid'], "source_dt": rec['source_dt'], "error": str(e)}, deltaBatch)
      format_strings = ','.join(['%s'] * len(failures))
      self.db_target.put(queryMap["geo_failure_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s', '%s')"%(str(rec['gid']), str(rec['source_dt']), str(rec['error'])), failures)))
      return None

  def streamDelta(self, batchSize, killer):
    start = int(round(time.time() * 1000))
    cur = self.db_target.getCursor(self.queries["geo_id_fetch"], (self.proc_id, self.procs, ))
    deltaBatch = cur.fetchmany(batchSize)
    jobs = []
    count = 0
    while len(deltaBatch)>0 and killer.runMore:
      ids = map(lambda rec: rec['gid'], deltaBatch)
      logger.info("shaping gids: "+json.dumps(ids, cls=Encoder))

      job = self.post(self.shaper.shape(ids), deltaBatch)
      if job is not None:
        jobs.append(job)
      count+=len(deltaBatch)
      deltaBatch = cur.fetchmany(batchSize)
    cur.close()
    map(lambda job: job.join(), jobs)
    end = int(round(time.time() * 1000))
    logger.info("batch [%s mod %s]%s record count: %s; processing time (ms): %s"%(str(self.proc_id), str(self.procs), "" if killer.runMore else " [killed]", str(count), str(end - start)))


