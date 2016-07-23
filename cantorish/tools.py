from HTMLParser import HTMLParser
import json
import decimal
import datetime
import MySQLdb
import MySQLdb.cursors
from config import Config
from itertools import groupby
import grequests
import logging
from queries import queryMap
from random import randint
import logging
import time
import phpserialize
from kafka import KafkaConsumer, KafkaClient, SimpleConsumer
import itertools
import copy

logger = logging.getLogger('etl_cantorish')

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
      init_command = "SET SESSION wait_timeout=6000;SET NAMES UTF8;SET time_zone='+0:00';SET SESSION group_concat_max_len = 1000000; SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
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


class CantorishShaper(object):
  db = None
  queryMap = None
  htmlParser = None

  def __init__(self, db, queryMap):
    self.db = db
    self.queryMap = queryMap
    self.htmlParser = HTMLParser()

  def ancestorCategoryIDList(self, ids, accumulator):
    if(len(ids)>0):
      accumulator.extend(ids)
      format_strings = ','.join(['%s'] * len(ids))
      parents = self.db.get(self.queryMap["parent_categories"] % format_strings, tuple(ids))
      self.ancestorCategoryIDList(filter(lambda cat: cat > 0, map(lambda cat: cat['parent_id'], parents)), accumulator)

  def ancestorCategories(self, leafids, product):
    parentids = []
    self.ancestorCategoryIDList(filter(lambda cat: cat > 0, map(lambda cat: cat['parent_id'], product['categories']['assigned'])), parentids)
    parentids = list(set(parentids) - set(leafids))

    if len(parentids) > 0:
      format_strings = ','.join(['%s'] * len(parentids))
      return self.db.get(self.queryMap["categories"] % format_strings, tuple(parentids))
    else:
      return []

  def subscribedProducts(self, id):
    products = []
    products = self.db.get(self.queryMap["subscribed_product"]%(id, ))
    for product in products:
      product['seller'] = {
        'id': product['store_id'],
        'code': product['store_code'],
        'name': product['store_name'],
        'tagline': product['tagline'],
        'description': product['store_details'],
        'media': [ { 'url': product['store_logo'], 'type': 'image' } ],
        'seller': product['seller_name'],
        'address': product['business_address'],
        'mobiles': filter(lambda num: num!="", map(lambda n: n.strip(), (product['mobile_numbers'] or "").split(','))),
        'telephones': filter(lambda num: num!="", map(lambda n: n.strip(), (product['telephone_numbers'] or "").split(','))),
        'email': product['email'],
        'chat_id': product['chat_id'],
        'visible': product['visible'],
        'status': product['store_status'],
        'is_deleted': product['store_is_deleted'],
        'is_active_valid': product['is_active_valid'],
        'vtiger_status': product['vtiger_status'],
        'vtiger_account_id': product['vtiger_account_id'],
        'meta': { 'title': product['meta_title'], 'keywords': product['meta_keywords'], 'description': product['meta_description'] },
        'customer_rating': product['customer_value'],
        'created_dt': product['store_created_dt'],
        'updated_dt': product['store_modified_dt'],
        'api_key': product['store_api_key'],
        'seller_mailer_flag': product['seller_mailer_flag'],
        'buyer_mailer_flag': product['buyer_mailer_flag'],
        'shipping_charge': product['store_shipping_charge']
      }
      product['offers'] = [ {
        'name': 'default',
        'discount_percent': product['default_discount_percent'],
        'price': product['store_offer_price'],
        'valid_from': '1970-01-01 00:00:00',
        'valid_thru': '9999-12-31 00:00:00'
      }, {
        'name': 'bazaar',
        'discount_percent': product['bazaar_discount_percent'],
        'price': product['bazaar_price'],
        'valid_from': product['bazaar_price_valid_from'],
        'valid_thru': product['bazaar_price_valid_thru']
      } ]
      for key in ['store_id', 'store_code', 'store_name', 'tagline', 'store_details', 'store_logo', 'seller_name', 'business_address', 'mobile_numbers', 'telephone_numbers', 'email', 'chat_id', 'visible', 'store_status', 'store_is_deleted', 'is_active_valid', 'vtiger_status', 'vtiger_account_id', 'meta_title', 'meta_keywords', 'meta_description', 'customer_value', 'store_created_dt', 'store_modified_dt', 'store_api_key', 'seller_mailer_flag', 'buyer_mailer_flag', 'store_shipping_charge', 'default_discount_percent', 'bazaar_discount_percent', 'bazaar_price_valid_from', 'bazaar_price_valid_thru']:
        product.pop(key, None)
    return products

  def isInt(self, i):
    try:
      int(i)
      return True
    except:
      return False

  def categoryTree(self, cats=[]):
    grouped = dict(map(lambda (k,v): (k, list(v)), itertools.groupby(sorted(cats, key=lambda c: c['parent_id']), lambda c: c['parent_id'])))

    for k, mcats in grouped.iteritems():
      for c in mcats:
        if c['id'] in grouped:
          c['tree']=grouped[c['id']]
            
    return grouped[0] if 0 in grouped else []

  def reshapeBase(self, product, is_top=True):
    id = product['id']
    product['name'] = self.htmlParser.unescape(product['name'] or "")
    product['description'] = self.htmlParser.unescape(product['description'] or "")
    product['small_description'] = self.htmlParser.unescape(product['small_description'] or "")

    product['meta'] = {}
    product['meta']['title'] = self.htmlParser.unescape(product['meta_title'] or "")
    product['meta']['keywords'] = self.htmlParser.unescape(product['meta_keyword'] or "")
    product['meta']['description'] = self.htmlParser.unescape(product['meta_description'] or "")
    product.pop('meta_title', None)
    product.pop('meta_keyword', None)
    product.pop('meta_description', None)

    product['media'] = self.db.get(self.queryMap["product_images"], (id, ))
    if is_top:
      product['categories'] = {}
      product['categories']['assigned'] = self.db.get(self.queryMap["product_categories"], (id, )) 
      product['categories']['derived'] = self.ancestorCategories(map(lambda cat: cat['id'], product['categories']['assigned']), product)
      all_cats = dict((v['id'], v) for v in product['categories']['assigned'] + product['categories']['derived']).values()
      for level in [0, 1, 2, 3, 4, 5, 6, 7]:
        cats = filter(lambda c: c['level'] == level, all_cats)
        if len(cats)>0:
          product['categories']['l'+str(level)] = cats
      product['categories']['all'] = all_cats
      product['categories']['tree'] = self.categoryTree(copy.deepcopy(all_cats))

    product['attributes'] = []
    attributes = []
    for att in ['color', 'size', 'brand', 'weight', 'model_name', 'model_number', 'key_features', 'manufacturer', 'video_url']:
      attributes.append({ 'name': att, 'value': product[att]})
      product.pop(att, None)
    try:
      for key, value in json.loads(product['specifications'])['attributes'].iteritems():
        attributes.append({ 'name': key, 'value': value })
    except:
      None
    product.pop('specifications', None)


    if is_top:
      subscriptions = self.subscribedProducts(id)
      product['variants'] = [{ 'id': id, 'created_dt': product['created_dt'], 'updated_dt': product['updated_dt'], 'name': product['name'], 'description': product['description'], 'small_description': product['small_description'], 'status': product['status'], 'is_deleted': product['is_deleted'], 'attributes': attributes, 'media': product['media'], 'meta': product['meta'], 'subscriptions': subscriptions}]
      product['subscriptions'] = subscriptions

      product['configurable_with'] = map(lambda i: int(i), filter(lambda i: self.isInt(i) and int(i) > 0, map(lambda c: c.strip(), (product['configurable_with'] or "").split(","))))
      if product['is_configurable'] == 1 and len(product['configurable_with']) > 0:
        format_strings = ','.join(['%s'] * len(product['configurable_with']))
        variants = self.db.get(self.queryMap["base_product"] % (format_strings) % tuple(product['configurable_with']))
        for variant in variants:
          product['variants'].append(self.reshapeBase(variant, is_top=False))
        common = set.intersection(*map(lambda variant: set(map(lambda a: json.dumps(a, cls=Encoder, indent=2), variant['attributes'])), variants))
        product['attributes'] = list(map(lambda a: json.loads(a), common))
        for variant in variants:
          variant['attributes'] = list(map(lambda a: json.loads(a), set(map(lambda a: json.dumps(a, cls=Encoder, indent=2), variant['attributes'])) - common))
       
      else:
        product['variants'][0]['attributes'] = []
        product['attributes'] = attributes
    else:
      product['attributes'] = attributes
      product['subscriptions'] = self.subscribedProducts(id)

    product.pop('configurable_with', None)
    product.pop('is_configurable', None)

    return product


  def shape(self, ids = []):
    format_strings = ','.join(['%s'] * len(ids))
    logger.debug(self.queryMap["base_product"] % (format_strings)% tuple(ids))
    products = self.db.get(self.queryMap["base_product"] % (format_strings), tuple(ids))
    logger.debug("products: "+json.dumps(products, cls=Encoder, indent=2)) 
    for product in products:
      product = self.reshapeBase(product)
    return products



class CantorishBaseDeltaUpdater(object):
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

    cur = self.db_source.getCursor(self.queries["cantorish_base_delta_fetch"], (idPrev, idCurr, ))
    deltaBatch = cur.fetchmany(batchSize)
    while len(deltaBatch)>0:
      format_strings = ','.join(['%s'] * len(deltaBatch))
      self.db_target.put(
        self.queries["cantorish_base_delta_merge"] % format_strings % 
        tuple(map(lambda rec: "(%s, '%s', %s)"%(str(rec['base_product_id']), str(rec['log_id']), str(randint(0, self.procs-1))), deltaBatch))
      )
      count+=len(deltaBatch)
      deltaBatch = cur.fetchmany(batchSize)

    cur.close()
    end = int(round(time.time() * 1000))
    self.db_target.put(self.queries["cantorish_base_bookmark_insert"], (idCurr, count, (end-start)))

  def idPrev(self): 
    result = self.db_target.get(self.queries["last_target_base_log_id"])
    return result[0]['log_id'] if len(result) > 0 else 0

  def idCurr(self):
    result = self.db_source.get(self.queries["max_source_base_log_id"])
    return result[0]['max_id'] if len(result) > 0 else 0


class CantorishSubscribedDeltaUpdater(object):
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

    cur = self.db_source.getCursor(self.queries["cantorish_subscribed_delta_fetch"], (idPrev, idCurr, idPrev, idCurr ))
    deltaBatch = cur.fetchmany(batchSize)
    while len(deltaBatch)>0:
      format_strings = ','.join(['%s'] * len(deltaBatch))
      self.db_target.put(
        self.queries["cantorish_subscribed_delta_merge"] % format_strings % 
        tuple(map(lambda rec: "(%s, '%s', %s)"%(str(rec['base_product_id']), str(rec['log_id']), str(randint(0, self.procs-1))), deltaBatch))
      )
      count+=len(deltaBatch)
      deltaBatch = cur.fetchmany(batchSize)

    cur.close()
    end = int(round(time.time() * 1000))
    self.db_target.put(self.queries["cantorish_subscribed_bookmark_insert"], (idCurr, count, (end-start)))

  def idPrev(self):
    result = self.db_target.get(self.queries["last_target_subscribed_log_id"])
    return result[0]['log_id'] if len(result) > 0 else 0

  def idCurr(self):
    result = self.db_source.get(self.queries["max_source_subscribed_log_id"])
    return result[0]['max_id'] if len(result) > 0 else 0



def hook_factory(*factory_args, **factory_kwargs):
  def updateStatus(result, *args, **kwargs):
    logger.info("target response: "+result.text)
    successes = []
    failures = []
    try:
      res = json.loads(result.text)
      if isinstance(res, dict):
        successes = filter(lambda rec: rec['id'] in map(lambda id: int(id), res['response']['successful']), factory_kwargs['delta_batch'])
        success_ids = map(lambda rec: rec['id'], successes)
        failed_list = filter(lambda rec: rec['id'] not in success_ids, factory_kwargs['delta_batch'])
        tgt_errors = dict((int(rec['id']), rec['error']) for rec in res['response']['failed'])
        failures = map(lambda rec: {"id": int(rec['id']), "error": tgt_errors[int(rec['id'])] if int(rec['id']) in tgt_errors else "no such id in source"}, failed_list)
      else:
        failures = map(lambda rec: {"id": rec['id'], "error": result.text}, factory_kwargs['delta_batch'])
    except Exception, e:
      logger.error("Exception: "+str(e))
      successes = []
      failures = map(lambda rec: {"id": rec['id'], "error": result.text}, factory_kwargs['delta_batch'])
 
    db = factory_kwargs['db']
    try:
      if len(successes) > 0:
        format_strings = ','.join(['%s'] * len(successes))
        db.put(queryMap["product_success_merge"] % format_strings % tuple(map(lambda rec: "(%s, %s, %s, %s, %s)"%(str(rec['id']), str(rec['source_base_log_id']), str(rec['source_subscribed_log_id']), str(rec['source_base_log_id']), str(rec['source_subscribed_log_id'])), successes)))
      if len(failures) > 0:
        format_strings = ','.join(['%s'] * len(failures))
        db.put(queryMap["product_failure_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s')"%(str(rec['id']), str(rec['error'])), failures)))
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
      req = grequests.post(self.url, data = json.dumps(data, cls=Encoder, indent=2), hooks = {'response': [hook_factory(delta_batch=deltaBatch, db=self.db_target)]})
      return grequests.send(req, self.requestPool)
    except Exception, e:
      failures = map(lambda rec: {"id": rec['id'], "error": str(e)}, deltaBatch)
      format_strings = ','.join(['%s'] * len(failures))
      self.db_target.put(queryMap["product_failure_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s')"%(str(rec['id']), str(rec['error'])), failures)))
      return None

  def streamDelta(self, batchSize, killer):
    start = int(round(time.time() * 1000))
    cur = self.db_target.getCursor(self.queries["product_id_fetch"], (self.proc_id, self.procs, ))
    deltaBatch = cur.fetchmany(batchSize)
    jobs = []
    count = 0
    while len(deltaBatch)>0 and killer.runMore:
      ids = map(lambda rec: rec['id'], deltaBatch)
      logger.info("shaping ids: "+json.dumps(ids, cls=Encoder))
      shape = self.shaper.shape(ids)
      logger.debug(json.dumps(shape, cls=Encoder, indent=2))
      job = self.post(shape, deltaBatch)
      if job is not None:
        jobs.append(job)
      count+=len(deltaBatch)
      deltaBatch = cur.fetchmany(batchSize)
    cur.close()
    map(lambda job: job.join(), jobs)
    end = int(round(time.time() * 1000))
    logger.info("batch [%s mod %s]%s record count: %s; processing time (ms): %s"%(str(self.proc_id), str(self.procs), "" if killer.runMore else " [killed]", str(count), str(end - start)))


