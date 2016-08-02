
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
import itertools
import copy

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


class ProductsShaper(object):
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
      self.ancestorCategoryIDList(filter(lambda cat: cat > 0, map(lambda cat: cat['parent_id'], filter(lambda p: p['category_id']!=0, parents))), accumulator)

  def ancestorCategories(self, leafids, product):
    parentids = []
    self.ancestorCategoryIDList(filter(lambda cat: cat > 0, map(lambda cat: cat['parent_id'], product['categories'])), parentids)
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

  def reshapeOptionsSubscribed(self, options):
    reshaped = []
    options.sort(key = lambda opt: opt['name'])
    for key, group in groupby(options, lambda x: { 'name': x['name'] , 'type': x['type'], 'sort_order': x['sort_order'], 'id': x['id'], 'base_product_id': x['base_product_id']}):
      key['values'] = map(lambda val: val['value'], group)
      reshaped.append(key)
    reshaped.sort(key = lambda opt: opt['sort_order'])
    return reshaped


  def subscribedProducts(self, ids, sellers = []):
    products = []
    if len(ids) > 0:
      ids_list = ','.join(map(lambda idi: str(idi), ids))
      format_strings = ','.join(['%s'] * len(ids))
      query = self.queryMap["subscribed_product"]%(ids_list, ids_list, )
      products = self.db.get(self.queryMap["subscribed_product"]%(ids_list, ids_list, ))

      for product in products:
        id = product['product_id']
        product['options'] = self.reshapeOptionsSubscribed(self.db.get(self.queryMap["subscribed_product_options"], (id, )))
        product['store_fronts'] = self.db.get(self.queryMap["subscribed_product_store_fronts"], (id, ))
        product['store_fronts_count'] = len(product['store_fronts'])
        product['images'] = map(lambda i: i['image'], self.db.get(self.queryMap["product_images"], (id, )))
        product['attributes'] = self.db.get(self.queryMap["base_product_attributes"], (id, ))
        mpdm_shit = self.db_mpdm.get(self.queryMap["mpdm_subscribed_product"], (product['subscribed_product_id'], ))
        if len(mpdm_shit) > 0:
          mpdm_shit = mpdm_shit[0]
          product['is_deleted'] = mpdm_shit['is_deleted']
          product['shipping_charge'] = mpdm_shit['shipping_charge']
          product['transfer_price'] = mpdm_shit['transfer_price']
          product['is_cod_apriori'] = True if mpdm_shit['is_cod_apriori'] > 0 else False
          product['order_margin'] = ((product['order_gsv']/product['order_quantity']) - product['transfer_price']) if product['order_quantity'] > 0 and product['order_gsv'] > 0 and product['transfer_price'] > 0 else 0
        else:
          product['is_deleted'] = True

        prices = self.db.get(self.queryMap["subscribed_special_price"], (id, ))
        product['flash_sales'] = []
        product['bazaar_prices'] = []
        product['selling_prices'] = []
        product['flash_sale_price'] = None
        product['selling_price'] = None
        product['bazaar_price'] = None
        for price in prices:
          if price['price'] > 0:
            if price['ecflashsale_id']:
              product['flash_sale_id'] = price['ecflashsale_id']
              product['flash_sale_price'] = price['price']
              product['flash_sale_start_date'] = price['date_start']
              product['flash_sale_end_date'] = price['date_end']
              flash_sale = {}
              flash_sale['id'] = price['ecflashsale_id']
              flash_sale['price'] = price['price']
              flash_sale['start_date'] = price['date_start']
              flash_sale['end_date'] = price['date_end']
              flash_sale['priority'] = price['priority']
              product['flash_sales'].append(flash_sale) 
            elif price['is_bazaar_price']:
              product['bazaar_price'] = price['price']
              product['bazaar_price_start_date'] = price['date_start']
              product['bazaar_price_end_date'] = price['date_end']
              bazaar_price = {}
              bazaar_price['price'] = price['price']
              bazaar_price['start_date'] = price['date_start']
              bazaar_price['end_date'] = price['date_end']
              bazaar_price['priority'] = price['priority']
              product['bazaar_prices'].append(bazaar_price)
            else:
              product['selling_price'] = price['price']
              product['selling_price_start_date'] = price['date_start']
              product['selling_price_end_date'] = price['date_end']
              selling_price = {}
              selling_price['price'] = price['price']
              selling_price['start_date'] = price['date_start']
              selling_price['end_date'] = price['date_end']
              selling_price['priority'] = price['priority']
              product['selling_prices'].append(selling_price)
             
        product['min_price'] = min(product['flash_sale_price'] or 1000000000000.0, product['bazaar_price'] or 1000000000000.0, product['selling_price'] or 1000000000000.0, product['price'])
        product['is_ndd'] = 1 if (product['seller_name'] or "").startswith('NDD ') else 0
        product['ndd_city'] = product['seller_name'][4:] if product['is_ndd'] else None
        product['is_cod'] = True if (int(product['min_price']) < 12000 and product['crm_seller_id'] not in sellers) else False
    return products

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

  def categoryTree(self, cats=[]):
    grouped = dict(map(lambda (k,v): (k, list(v)), itertools.groupby(sorted(cats, key=lambda c: c['parent_id']), lambda c: c['parent_id'])))

    for k, mcats in grouped.iteritems():
      for c in mcats:
        if c['category_id'] in grouped:
          c['categories_tree']=filter(lambda ic: ic['category_id']!=c['category_id'], grouped[c['category_id']])

    return grouped[0] if 0 in grouped else []

  def shape(self, ids = []):
    format_strings = ','.join(['%s'] * len(ids))
    logger.debug(self.queryMap["base_product"] % (format_strings)% tuple(ids))
    products = self.db.get(self.queryMap["base_product"] % (format_strings), tuple(ids))
    settings = map(lambda e: phpserialize.loads(e['value']), self.db.get(self.queryMap["settings"]))
    sellers = map(lambda s: int(s), [seller for sublist in settings for seller in sublist])
    logger.debug("products: "+json.dumps(products, cls=Encoder, indent=2)) 
    for product in products:
      id = product['product_id']
      mpdm_shit = self.db_mpdm.get(self.queryMap["mpdm_base_product"], (product['base_product_id'], ))
      product['is_deleted'] = mpdm_shit[0]['is_deleted'] if len(mpdm_shit) > 0 else True
      product['categories'] = self.db.get(self.queryMap["product_categories"], (id, )) 
      product['parent_categories'] = self.ancestorCategories(map(lambda cat: cat['category_id'], product['categories']), product)
      self.levelCategories(product['categories'], product['parent_categories'])
      all_cats = dict((v['category_id'], v) for v in product['categories'] + product['parent_categories']).values()
      product['categories_l1'] = filter(lambda c: c['level'] == 1, all_cats)
      product['categories_l2'] = filter(lambda c: c['level'] == 2, all_cats)
      product['categories_l3'] = filter(lambda c: c['level'] == 3, all_cats)
      product['categories_l4'] = filter(lambda c: c['level'] == 4, all_cats)
      product['categories_l5'] = filter(lambda c: c['level'] == 5, all_cats)
      product['categories_l6'] = filter(lambda c: c['level'] == 6, all_cats)
      product['categories_l7'] = filter(lambda c: c['level'] == 7, all_cats)
      product['categories_nested'] = all_cats
      product['categories_tree'] = self.categoryTree(copy.deepcopy(all_cats))
      product['options'] = self.reshapeOptions(self.db.get(self.queryMap["base_product_options"], (id, )))
      product['attributes'] = self.db.get(self.queryMap["base_product_attributes"], (id, ))
      product['attributes_value'] = []
      for attribute in product['attributes']:
        if attribute['name'].startswith('Filter'):
          product['attributes_value'].append(attribute['value'])
      product['stores'] = self.db.get(self.queryMap["base_product_stores"], (id, ))
      product['reviews'] = self.db.get(self.queryMap["base_product_reviews"], (id, ))
      subscribed_ids = map(lambda rec: rec['grouped_id'], self.db.get(self.queryMap["subscribed_ids"], (id, )))
      product['subscriptions'] = self.subscribedProducts(subscribed_ids, sellers)
      product['seller_name'] = map(lambda s: s['seller_name'], filter(lambda s: s['status'] == 1, product['subscriptions']))
      product['order_count'] = sum(map(lambda s: s['order_count'], product['subscriptions'])) if len(product['subscriptions']) > 0 else 0.0
      product['order_quantity'] = sum(map(lambda s: s['order_quantity'], product['subscriptions'])) if len(product['subscriptions']) > 0 else 0.0
      product['order_gsv'] = sum(map(lambda s: s['order_gsv'], product['subscriptions'])) if len(product['subscriptions']) > 0 else 0.0
      product['order_loyalty_earned'] = sum(map(lambda s: s['order_loyalty_earned'], product['subscriptions'])) if len(product['subscriptions']) > 0 else 0
      order_dates = filter(lambda d: d is not None, map(lambda s: s['order_last_dt'], product['subscriptions']))
      product['order_last_dt'] = max(order_dates) if len(order_dates) > 0 else None
      product['order_discount_pct_avg'] = max(map(lambda s: s['order_discount_pct_avg'], product['subscriptions'])) if len(product['subscriptions']) > 0 else 0.0
      product['order_margin'] = max(map(lambda s: s['order_margin'] if 'order_margin' in s else 0, product['subscriptions'])) if len(product['subscriptions']) > 0 else 0.0
      product['store_fronts_count'] = max(map(lambda s: s['store_fronts_count'], product['subscriptions'])) if len(product['subscriptions']) > 0 else 0.0
      product['images'] = map(lambda i: i['image'], self.db.get(self.queryMap["product_images"], (id, )))
      product['min_price'] = min(map(lambda sub: sub['min_price'], product['subscriptions'])) if(len(product['subscriptions'])>0) else None
      product.pop('subscribed_product_ids', None)
      ids.remove(id)
    for id in ids:
      products.append({'product_id': id, 'base_product_id': 0, 'is_deleted': True})
    return products


class ProductCustomDeltaUpdater(object):
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
    count = 0
    cur = self.db_source.getCursor(self.queries["product_custom_fetch"])
    deltaBatch = cur.fetchmany(batchSize)
    while len(deltaBatch)>0:
      format_strings = ','.join(['%s'] * len(deltaBatch))
      query = self.queries["product_custom_merge"] % format_strings % tuple(map(lambda rec: str(rec['product_id']), deltaBatch))
      logger.info("custom query: "+query)
      self.db_target.put(query)
      count+=len(deltaBatch)
      deltaBatch = cur.fetchmany(batchSize)

    cur.close()
    end = int(round(time.time() * 1000))

class ProductDeltaUpdater(object):
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
    cur = self.db_source.getCursor(self.queries["product_delta_fetch"], (idPrev, idCurr, ))
    deltaBatch = cur.fetchmany(batchSize)
    while len(deltaBatch)>0:
      format_strings = ','.join(['%s'] * len(deltaBatch))
      self.db_target.put(
        self.queries["product_delta_merge"] % format_strings % 
        tuple(map(lambda rec: "(%s, '%s', %s)"%(str(rec['product_id']), str(rec['last_updated_dt']), str(randint(0, self.procs-1))), deltaBatch))
      )
      count+=len(deltaBatch)
      deltaBatch = cur.fetchmany(batchSize)

    cur.close()
    end = int(round(time.time() * 1000))
    self.db_target.put(self.queries["product_bookmark_insert"], (idCurr, count, (end-start)))

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
        successes = filter(lambda rec: rec['product_id'] in map(lambda id: int(id), res['response']['successful']), factory_kwargs['delta_batch'])
        success_ids = map(lambda rec: rec['product_id'], successes)
        failed_list = filter(lambda rec: rec['product_id'] not in success_ids, factory_kwargs['delta_batch'])
        tgt_errors = dict((int(rec['product_id']), rec['error']) for rec in res['response']['failed'])
        failures = map(lambda rec: {"product_id": int(rec['product_id']), "source_dt": rec['source_dt'], "error": tgt_errors[int(rec['product_id'])] if int(rec['product_id']) in tgt_errors else "no such grouped product_id in source"}, failed_list)
      else:
        failures = map(lambda rec: {"product_id": rec['product_id'], "source_dt": rec['source_dt'], "error": result.text}, factory_kwargs['delta_batch'])
    except Exception, e:
      logger.error("Exception: "+str(e))
      successes = []
      failures = map(lambda rec: {"product_id": rec['product_id'], "source_dt": rec['source_dt'], "error": result.text}, factory_kwargs['delta_batch'])
 
    db = factory_kwargs['db']
    try:
      if len(successes) > 0:
        format_strings = ','.join(['%s'] * len(successes))
        db.put(queryMap["product_success_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s', '%s')"%(str(rec['product_id']), str(rec['source_dt']), str(rec['source_dt'])), successes)))
      if len(failures) > 0:
        format_strings = ','.join(['%s'] * len(failures))
        db.put(queryMap["product_failure_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s', '%s')"%(str(rec['product_id']), str(rec['source_dt']), str(rec['error'])), failures)))
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
      failures = map(lambda rec: {"product_id": rec['product_id'], "source_dt": rec['source_dt'], "error": str(e)}, deltaBatch)
      format_strings = ','.join(['%s'] * len(failures))
      self.db_target.put(queryMap["product_failure_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s', '%s')"%(str(rec['product_id']), str(rec['source_dt']), str(rec['error'])), failures)))
      return None

  def streamDelta(self, batchSize, killer):
    start = int(round(time.time() * 1000))
    cur = self.db_target.getCursor(self.queries["product_id_fetch"], (self.proc_id, self.procs, ))
    deltaBatch = cur.fetchmany(batchSize)
    jobs = []
    count = 0
    while len(deltaBatch)>0 and killer.runMore:
      ids = map(lambda rec: rec['product_id'], deltaBatch)
      logger.info("shaping product_ids: "+json.dumps(ids, cls=Encoder))
      job = self.post(self.shaper.shape(ids), deltaBatch)
      if job is not None:
        jobs.append(job)
      count+=len(deltaBatch)
      deltaBatch = cur.fetchmany(batchSize)
    cur.close()
    map(lambda job: job.join(), jobs)
    end = int(round(time.time() * 1000))
    logger.info("batch [%s mod %s]%s record count: %s; processing time (ms): %s"%(str(self.proc_id), str(self.procs), "" if killer.runMore else " [killed]", str(count), str(end - start)))


