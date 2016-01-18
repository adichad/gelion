
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


class Encoder(json.JSONEncoder):
  def default(self, o):
    if isinstance(o, decimal.Decimal):
      return float(o)
    if isinstance(o, datetime.date):
      return o.strftime("%Y-%m-%d %H:%M:%S")
    return super(Encoder, self).default(o)


class BaseDB(object):
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
      charset = 'utf8mb4',
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


class ProductsShaper(object):
  db = None
  queryMap = None

  def __init__(self, db, queryMap):
    self.db = db
    self.queryMap = queryMap

  def ancestorCategoryIDList(self, ids, accumulator):
    if(len(ids)>0):
      accumulator.extend(ids)
      format_strings = ','.join(['%s'] * len(ids))
      parents = self.db.get(self.queryMap["parent_categories"] % format_strings, tuple(ids))
      self.ancestorCategoryIDList(filter(lambda cat: cat > 0, map(lambda cat: cat['parent_id'], parents)), accumulator)

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
      key['values'] = list(set(map(lambda val: val['value'], group)))
      reshaped.append(key)
    return reshaped

  def subscribedProducts(self, ids):
    products = []
    if len(ids) > 0:
      format_strings = ','.join(['%s'] * len(ids))
      products = self.db.get(self.queryMap["subscribed_product"] % format_strings, tuple(ids))

      for product in products:
        id = product['product_id']

        product['options'] = self.reshapeOptions(self.db.get(self.queryMap["subscribed_product_options"], (id, )))
        prices = self.db.get(self.queryMap["subscribed_special_price"], (id, ))

        product['flash_sale_price'] = None
        product['selling_price'] = None
        for price in prices:
          if price['ecflashsale_id']:
            product['flash_sale_id'] = price['ecflashsale_id']
            product['flash_sale_price'] = price['price']
            product['flash_sale_start_date'] = price['date_start']
            product['flash_sale_end_date'] = price['date_end']
          else:
            product['selling_price'] = price['price'] 
            #TOASK: if a flash sale is not pri-1 or pri-2 or not among the 2 lowest special prices, 
            #then use second priority/second lowest special price (non-flash) as the selling price?

        product['min_price'] = min(product['flash_sale_price'] or 1000000000000.0, product['selling_price'] or 1000000000000.0, product['price'])
        product['is_ndd'] = True if product['seller_name'].startswith('NDD ') else False
        product['ndd_city'] = product['seller_name'][4:] if product['is_ndd'] else None

    return products


  def shape(self, ids = []):
    format_strings = ','.join(['%s'] * len(ids))
    #print(self.queryMap["base_product"] % (format_strings, format_strings)% tuple(ids))
    products = self.db.get(self.queryMap["base_product"] % (format_strings), tuple(ids))
    
    for product in products:
      id = product['product_id']
      product['categories'] = self.db.get(self.queryMap["product_categories"], (id, )) #TOASK: Category assigment is non-mandatory? 1165
      product['parent_categories'] = self.ancestorCategories(map(lambda cat: cat['category_id'], product['categories']), product)
      product['options'] = self.reshapeOptions(self.db.get(self.queryMap["base_product_options"], (id, )))
      product['attributes'] = self.db.get(self.queryMap["base_product_attributes"], (id, ))
      product['stores'] = self.db.get(self.queryMap["base_product_stores"], (id, ))
      product['reviews'] = self.db.get(self.queryMap["base_product_reviews"], (id, ))
      subscribed_ids = map(lambda rec: rec['grouped_id'], self.db.get(self.queryMap["subscribed_ids"], (id, )))
      product['subscriptions'] = self.subscribedProducts(subscribed_ids)

      product['images'] = product['images'].split(',')
      product['min_price'] = min(map(lambda sub: sub['min_price'], product['subscriptions'])) if(len(product['subscriptions'])>0) else None
      product.pop('subscribed_product_ids', None)
      
    return products



class ProductDeltaUpdater(object):
  db_source = None
  db_target = None
  queries = None

  def __init__(self, db_source, db_target, queries):
    self.db_source = db_source
    self.db_target = db_target
    self.queries = queries

  def streamDelta(self, batchSize):
    idPrev = self.idPrev()
    idCurr = self.idCurr()
    print(str(idPrev))
    print(str(idCurr))
    count = 0
    cur = self.db_source.getCursor(self.queries["product_delta_fetch"], (idPrev, idCurr, ))
    deltaBatch = cur.fetchmany(batchSize)
    while len(deltaBatch)>0:
      format_strings = ','.join(['%s'] * len(deltaBatch))
      self.db_target.put(
        self.queries["product_delta_merge"] % format_strings % 
        tuple(map(lambda rec: "(%s, '%s')"%(str(rec['product_id']), str(rec['last_updated_dt'])), deltaBatch))
      )
      count += len(deltaBatch)
      deltaBatch = cur.fetchmany(batchSize)

    cur.close()
    self.db_target.put(self.queries["product_bookmark_insert"], (idCurr, count, ))

  def idPrev(self): 
    result = self.db_target.get(self.queries["last_target_log_id"])
    return result[0]['log_id'] if len(result) > 0 else 0

  def idCurr(self):
    result = self.db_source.get(self.queries["max_source_log_id"])
    return result[0]['max_id'] if len(result) > 0 else 0



def hook_factory(*factory_args, **factory_kwargs):
  def updateStatus(result, *args, **kwargs):
    try:
      res = json.loads(result.text)
      successes = filter(lambda rec: rec['product_id'] in map(lambda id: int(id), res['response']['successful']), factory_kwargs['delta_batch'])
      failures = map(lambda rec: {"product_id": int(rec['product_id']), "error": rec['error']}, res['response']['failed'])
    except:
      successes = []
      failures = map(lambda rec: {"product_id": rec['product_id'], "error": result.text}, factory_kwargs['delta_batch'])
    
    db = factory_kwargs['db']
    if len(successes) > 0:
      format_strings = ','.join(['%s'] * len(successes))
      db.put(queryMap["product_success_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s', '%s')"%(str(rec['product_id']), str(rec['source_dt']), str(rec['source_dt'])), successes)))
    if len(failures) > 0:
      format_strings = ','.join(['%s'] * len(failures))
      db.put(queryMap["product_failure_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s', '%s')"%(str(rec['product_id']), str(rec['source_dt']), str(rec['error'])), failures)))
    result.close()
    return None
  return updateStatus


class MandelbrotPipe(object):
  db_source = None
  db_target = None
  queries = None
  idStart = None
  idEnd = None
  shaper = None
  url = None
  requestPool = None

  def __init__(self, db_source, db_target, queries, idStart, idEnd, shaper, url, requestPool):
    self.db_source = db_source
    self.db_target = db_target
    self.queries = queries
    self.idStart = idStart
    self.idEnd = idEnd
    self.shaper = shaper
    self.url = url
    self.requestPool = requestPool

  
  def post(self, data, deltaBatch):
    try:
      req = grequests.post(self.url, data = json.dumps(data, cls=Encoder, indent=2), hooks = {'response': [hook_factory(delta_batch=deltaBatch, db=self.db_target)]})
      job = grequests.send(req, self.requestPool)
    except Exception, e:
      failures = map(lambda rec: {"product_id": rec['product_id'], "source_dt": rec['source_dt'], "error": str(e)}, deltaBatch)
      format_strings = ','.join(['%s'] * len(failures))
      self.db_target.put(queryMap["product_failure_merge"] % format_strings % tuple(map(lambda rec: "(%s, '%s', '%s')"%(str(rec['product_id']), str(rec['source_dt']), str(rec['error'])), failures)))

  def streamDelta(self, batchSize):
    cur = self.db_target.getCursor(self.queries["product_id_fetch"], (self.idStart, self.idEnd, ))
    deltaBatch = cur.fetchmany(batchSize)
    while len(deltaBatch)>0:
      ids = map(lambda rec: rec['product_id'], deltaBatch)
      print(json.dumps(ids, cls=Encoder))
      self.post(self.shaper.shape(ids), deltaBatch)
      deltaBatch = cur.fetchmany(batchSize)

    cur.close()

