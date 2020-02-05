
#import json
from pymongo import MongoClient
import pandas as pd

###connect to MongoDB
client = MongoClient('mongodb://localhost:27017/test')
db = client['DB_NAME']
mycoll= db['Collection_name']


#following is the example:
mongo_query = db.collection.find({"profileDetails.customerCategory.masterCode" : "PRECAT1"}, {"_id": 0})

##function read the  pymongo cursor and iterate thru it
### the function is ccoopreate wth https://github.com/sina33

def flatten(d, parent_key='', sep='.'):
    items = {}
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collectionsAbc.MutableMapping):
            items.update(flatten(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, e in enumerate(v):
                items.update(flatten({new_key + sep + str(i): v[i]}))
        else:
            items.update({new_key: v})
    return dict(items)


ndf = pd.DataFrame()
for doc in mongo_query:
    nd = {}
    d = find_dicts(doc)
    df = pd.DataFrame(nd , index=[0])
    ndf = pd.concat([ndf , df],ignore_index=True)
ndf.to_csv('Finall.csv', index=False)

