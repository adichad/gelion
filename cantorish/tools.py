
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
      init_command = "SET SESSION wait_timeout=60;SET NAMES UTF8;SET time_zone='+0:00';SET SESSION group_concat_max_len = 1000000; SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;",
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
  db_mpdm = None
  queryMap = None

  def __init__(self, db, db_mpdm, queryMap):
    self.db = db
    self.db_mpdm = db_mpdm
    self.queryMap = queryMap

  def ancestorCategoryIDList(self, ids, accumulator):
    if(len(ids)>0):
      accumulator.extend(ids)
      format_strings = ','.join(['%s'] * len(ids))
      parents = self.db.get(self.queryMap["parent_categories"] % format_strings, tuple(ids))
      self.ancestorCategoryIDList(filter(lambda cat: cat > 0, map(lambda cat: cat['parent_id'], parents)), accumulator)

  def ancestorCategories(self, leafids, cantorish):
    parentids = []
    self.ancestorCategoryIDList(filter(lambda cat: cat > 0, map(lambda cat: cat['parent_id'], cantorish['categories'])), parentids)
    parentids = list(set(parentids) - set(leafids))

    if len(parentids) > 0:
      format_strings = ','.join(['%s'] * len(parentids))
      return self.db.get(self.queryMap["categories"] % format_strings, tuple(parentids))
    else:
      return []

  def reshapeOptions(self, options):
    reshaped = []
    options.sort(key = lambda opt: opt['name'])
    for key, group in groupby(options, lambda x: { 'name': x['name'] , 'type': x['type'], 'sort_order': x['sort_order'], 'id': x['id']}):
      key['values'] = map(lambda val: val['value'], group)
      reshaped.append(key)
    reshaped.sort(key = lambda opt: opt['sort_order'])
    return reshaped


  def subscribedCantorishs(self, ids, sellers = []):
    cantorishs = []
    if len(ids) > 0:
      ids_list = ','.join(map(lambda idi: str(idi), ids))
      format_strings = ','.join(['%s'] * len(ids))
      query = self.queryMap["subscribed_cantorish"]%(ids_list, ids_list, )
      cantorishs = self.db.get(self.queryMap["subscribed_cantorish"]%(ids_list, ids_list, ))

      for cantorish in cantorishs:
        id = cantorish['cantorish_id']
        cantorish['options'] = self.reshapeOptions(self.db.get(self.queryMap["subscribed_cantorish_options"], (id, )))
        cantorish['store_fronts'] = self.db.get(self.queryMap["subscribed_cantorish_store_fronts"], (id, ))
        cantorish['store_fronts_count'] = len(cantorish['store_fronts'])
        cantorish['images'] = map(lambda i: i['image'], self.db.get(self.queryMap["cantorish_images"], (id, )))
        mpdm_shit = self.db_mpdm.get(self.queryMap["mpdm_subscribed_cantorish"], (cantorish['subscribed_cantorish_id'], ))
        if len(mpdm_shit) > 0:
          mpdm_shit = mpdm_shit[0]
          cantorish['shipping_charge'] = mpdm_shit['shipping_charge']
          cantorish['transfer_price'] = mpdm_shit['transfer_price']
          cantorish['is_cod_apriori'] = True if mpdm_shit['is_cod_apriori'] > 0 else False
          cantorish['order_margin'] = ((cantorish['order_gsv']/cantorish['order_quantity']) - cantorish['transfer_price']) if cantorish['order_quantity'] > 0 and cantorish['order_gsv'] > 0 and cantorish['transfer_price'] > 0 else 0

        prices = self.db.get(self.queryMap["subscribed_special_price"], (id, ))

        cantorish['flash_sales'] = []
        cantorish['bazaar_prices'] = []
        cantorish['selling_prices'] = []
        cantorish['flash_sale_price'] = None
        cantorish['selling_price'] = None
        cantorish['bazaar_price'] = None
        for price in prices:
          if price['price'] > 0:
            if price['ecflashsale_id']:
              cantorish['flash_sale_id'] = price['ecflashsale_id']
              cantorish['flash_sale_price'] = price['price']
              cantorish['flash_sale_start_date'] = price['date_start']
              cantorish['flash_sale_end_date'] = price['date_end']
              flash_sale = {}
              flash_sale['id'] = price['ecflashsale_id']
              flash_sale['price'] = price['price']
              flash_sale['start_date'] = price['date_start']
              flash_sale['end_date'] = price['date_end']
              flash_sale['priority'] = price['priority']
              cantorish['flash_sales'].append(flash_sale) 
            elif price['is_bazaar_price']:
              cantorish['bazaar_price'] = price['price']
              cantorish['bazaar_price_start_date'] = price['date_start']
              cantorish['bazaar_price_end_date'] = price['date_end']
              bazaar_price = {}
              bazaar_price['price'] = price['price']
              bazaar_price['start_date'] = price['date_start']
              bazaar_price['end_date'] = price['date_end']
              bazaar_price['priority'] = price['priority']
              cantorish['bazaar_prices'].append(bazaar_price)
            else:
              cantorish['selling_price'] = price['price']
              cantorish['selling_price_start_date'] = price['date_start']
              cantorish['selling_price_end_date'] = price['date_end']
              selling_price = {}
              selling_price['price'] = price['price']
              selling_price['start_date'] = price['date_start']
              selling_price['end_date'] = price['date_end']
              selling_price['priority'] = price['priority']
              cantorish['selling_prices'].append(selling_price)
             
        cantorish['min_price'] = min(cantorish['flash_sale_price'] or 1000000000000.0, cantorish['bazaar_price'] or 1000000000000.0, cantorish['selling_price'] or 1000000000000.0, cantorish['price'])
        cantorish['is_ndd'] = 1 if (cantorish['seller_name'] or "").startswith('NDD ') else 0
        cantorish['ndd_city'] = cantorish['seller_name'][4:] if cantorish['is_ndd'] else None
        cantorish['is_cod'] = True if (int(cantorish['min_price']) < 12000 and cantorish['crm_seller_id'] not in sellers) else False
    return cantorishs

  def level(self, category, parent_categories):
    parents = filter(lambda p: p['category_id'] == category['parent_id'], parent_categories)
    if len(parents) == 0:
      category['level'] = 1
    else:
      parent = parents[0]
      if 'level' not in parent:
        self.level(parent, parent_categories)
      category['level'] = parent['level']+1

  def levelCategories(self, categories = [], parent_categories = []):
    for category in categories:
      self.level(category, parent_categories)

  def shape(self, ids = []):
    format_strings = ','.join(['%s'] * len(ids))
    logger.debug(self.queryMap["base_cantorish"] % (format_strings)% tuple(ids))
    cantorishs = self.db.get(self.queryMap["base_cantorish"] % (format_strings), tuple(ids))
    settings = map(lambda e: phpserialize.loads(e['value']), self.db.get(self.queryMap["settings"]))
    sellers = map(lambda s: int(s), [seller for sublist in settings for seller in sublist])
    logger.debug("cantorishs: "+json.dumps(cantorishs, cls=Encoder, indent=2)) 
    for cantorish in cantorishs:
      id = cantorish['cantorish_id']
      cantorish['categories'] = self.db.get(self.queryMap["cantorish_categories"], (id, )) #TOASK: Category assigment is non-mandatory? 1165
      cantorish['parent_categories'] = self.ancestorCategories(map(lambda cat: cat['category_id'], cantorish['categories']), cantorish)
      self.levelCategories(cantorish['categories'], cantorish['parent_categories'])
      all_cats = dict((v['category_id'], v) for v in cantorish['categories'] + cantorish['parent_categories']).values()
      cantorish['categories_l1'] = filter(lambda c: c['level'] == 1, all_cats)
      cantorish['categories_l2'] = filter(lambda c: c['level'] == 2, all_cats)
      cantorish['categories_l3'] = filter(lambda c: c['level'] == 3, all_cats)
      cantorish['categories_l4'] = filter(lambda c: c['level'] == 4, all_cats)
      cantorish['categories_l5'] = filter(lambda c: c['level'] == 5, all_cats)
      cantorish['categories_l6'] = filter(lambda c: c['level'] == 6, all_cats)
      cantorish['categories_l7'] = filter(lambda c: c['level'] == 7, all_cats)
      cantorish['categories_nested'] = all_cats
      cantorish['options'] = self.reshapeOptions(self.db.get(self.queryMap["base_cantorish_options"], (id, )))
      cantorish['attributes'] = self.db.get(self.queryMap["base_cantorish_attributes"], (id, ))
      cantorish['attributes_value'] = []
      for attribute in cantorish['attributes']:
        if attribute['name'].startswith('Filter'):
          cantorish['attributes_value'].append(attribute['value'])
      cantorish['stores'] = self.db.get(self.queryMap["base_cantorish_stores"], (id, ))
      cantorish['reviews'] = self.db.get(self.queryMap["base_cantorish_reviews"], (id, ))
      subscribed_ids = map(lambda rec: rec['grouped_id'], self.db.get(self.queryMap["subscribed_ids"], (id, )))
      cantorish['subscriptions'] = self.subscribedCantorishs(subscribed_ids, sellers)

      cantorish['order_count'] = sum(map(lambda s: s['order_count'], cantorish['subscriptions'])) if len(cantorish['subscriptions']) > 0 else 0.0
      cantorish['order_quantity'] = sum(map(lambda s: s['order_quantity'], cantorish['subscriptions'])) if len(cantorish['subscriptions']) > 0 else 0.0
      cantorish['order_gsv'] = sum(map(lambda s: s['order_gsv'], cantorish['subscriptions'])) if len(cantorish['subscriptions']) > 0 else 0.0
      cantorish['order_loyalty_earned'] = sum(map(lambda s: s['order_loyalty_earned'], cantorish['subscriptions'])) if len(cantorish['subscriptions']) > 0 else 0
      order_dates = filter(lambda d: d is not None, map(lambda s: s['order_last_dt'], cantorish['subscriptions']))
      cantorish['order_last_dt'] = max(order_dates) if len(order_dates) > 0 else None
      cantorish['order_discount_pct_avg'] = max(map(lambda s: s['order_discount_pct_avg'], cantorish['subscriptions'])) if len(cantorish['subscriptions']) > 0 else 0.0
      cantorish['order_margin'] = max(map(lambda s: s['order_margin'] if 'order_margin' in s else 0, cantorish['subscriptions'])) if len(cantorish['subscriptions']) > 0 else 0.0
      cantorish['store_fronts_count'] = max(map(lambda s: s['store_fronts_count'], cantorish['subscriptions'])) if len(cantorish['subscriptions']) > 0 else 0.0
      cantorish['images'] = map(lambda i: i['image'], self.db.get(self.queryMap["cantorish_images"], (id, )))
      cantorish['min_price'] = min(map(lambda sub: sub['min_price'], cantorish['subscriptions'])) if(len(cantorish['subscriptions'])>0) else None
      cantorish.pop('subscribed_cantorish_ids', None)
      
    return cantorishs



class CantorishDeltaUpdater(object):
  db_source = None
  db_target = None
  queries = None
  procs = None
  consumer = None

  def __init__(self, db_source, db_target, kafka_consumer, queries, procs):
    self.db_source = db_source
    self.db_target = db_target
    self.queries = queries
    self.procs = procs
    self.consumer = kafka_consumer

  def streamDelta(self, batchSize):
    start = int(round(time.time() * 1000))

    topical_messages = self.consumer.poll(0)

    logger.info(json.dumps(topical_messages, cls=Encoder, indent=2))

#    idPrev = self.idPrev()
#    idCurr = self.idCurr()
#    logger.info("last processed log_id: "+str(idPrev))
#    logger.info("current max    log_id: "+str(idCurr))
#    count = 0

#    cur = self.db_source.getCursor(self.queries["cantorish_delta_fetch"], (idPrev, idCurr, ))
#    deltaBatch = cur.fetchmany(batchSize)
#    while len(deltaBatch)>0:
#      format_strings = ','.join(['%s'] * len(deltaBatch))
#      self.db_target.put(
#        self.queries["cantorish_delta_merge"] % format_strings % 
#        tuple(map(lambda rec: "(%s, '%s', %s)"%(str(rec['cantorish_id']), str(rec['last_updated_dt']), str(randint(0, self.procs-1))), deltaBatch))
#      )
#      count+=len(deltaBatch)
#      deltaBatch = cur.fetchmany(batchSize)

#    cur.close()
    end = int(round(time.time() * 1000))
#    self.db_target.put(self.queries["cantorish_bookmark_insert"], (idCurr, count, (end-start)))

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
        successes = filter(lambda rec: rec['cantorish_id'] in map(lambda id: int(id), res['response']['successful']), factory_kwargs['delta_batch'])
        success_ids = map(lambda rec: rec['cantorish_id'], successes)
        failed_list = filter(lambda rec: rec['cantorish_id'] not in success_ids, factory_kwargs['delta_batch'])
        tgt_errors = dict((int(rec['cantorish_id']), rec['error']) for rec in res['response']['failed'])
        failures = map(lambda rec: {"cantorish_id": int(rec['cantorish_id']), "source_dt": rec['source_dt'], "error": tgt_errors[int(rec['cantorish_id'])] if int(rec['cantorish_id']) in tgt_errors else "no such grouped cantorish_id in source"}, failed_list)
      else:
        failures = map(lambda rec: {"cantorish_id": rec['cantorish_id'], "source_dt": rec['source_dt'], "error": result.text}, factory_kwargs['delta_batch'])
    except Exception, e:
      logger.error("Exception: "+str(e))
      successes = []
      failures = map(lambda rec: {"cantorish_id": rec['cantorish_id'], "source_dt": rec['source_dt'], "error": result.text}, factory_kwargs['delta_batch'])
 
    db = factory_kwargs['db']
    try:
      if len(successes) > 0:
        format_strings = ','.join(['%s'] * len(successes))
        db.put(queryMap["cantorish_success_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s', '%s')"%(str(rec['cantorish_id']), str(rec['source_dt']), str(rec['source_dt'])), successes)))
      if len(failures) > 0:
        format_strings = ','.join(['%s'] * len(failures))
        db.put(queryMap["cantorish_failure_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s', '%s')"%(str(rec['cantorish_id']), str(rec['source_dt']), str(rec['error'])), failures)))
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
      failures = map(lambda rec: {"cantorish_id": rec['cantorish_id'], "source_dt": rec['source_dt'], "error": str(e)}, deltaBatch)
      format_strings = ','.join(['%s'] * len(failures))
      self.db_target.put(queryMap["cantorish_failure_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s', '%s')"%(str(rec['cantorish_id']), str(rec['source_dt']), str(rec['error'])), failures)))
      return None

  def streamDelta(self, batchSize, killer):
    start = int(round(time.time() * 1000))
    cur = self.db_target.getCursor(self.queries["cantorish_id_fetch"], (self.proc_id, self.procs, ))
    deltaBatch = cur.fetchmany(batchSize)
    jobs = []
    count = 0
    while len(deltaBatch)>0 and killer.runMore:
      ids = map(lambda rec: rec['cantorish_id'], deltaBatch)
      logger.info("shaping cantorish_ids: "+json.dumps(ids, cls=Encoder))
      job = self.post(self.shaper.shape(ids), deltaBatch)
      if job is not None:
        jobs.append(job)
      count+=len(deltaBatch)
      deltaBatch = cur.fetchmany(batchSize)
    cur.close()
    map(lambda job: job.join(), jobs)
    end = int(round(time.time() * 1000))
    logger.info("batch [%s mod %s]%s record count: %s; processing time (ms): %s"%(str(self.proc_id), str(self.procs), "" if killer.runMore else " [killed]", str(count), str(end - start)))


