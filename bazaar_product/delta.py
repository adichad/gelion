
from config import Config
import json
from tools import BaseDB, Encoder, ProductDeltaUpdater
from queries import queryMap



cfg = Config(file('simple.cfg'))

db_source = BaseDB(cfg['default']['db']['source'])
db_target = BaseDB(cfg['default']['db']['management'])


deltaUpdater = ProductDeltaUpdater(db_source, db_target, queryMap)
deltaUpdater.streamDelta(1000)

#shaper = ProductsShaper(db_source, queryMap)
#print(json.dumps(shaper.shape([32561, 8588, 309533]), cls=Encoder, indent=2))
db_source.close()
db_target.close()

