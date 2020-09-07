#!/usr/bin/env python
# coding: utf-8

# In[26]:





# In[27]:


import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO) #DEBUG, INFO, WARNING, ERROR, CRITICAL

#!pip install --user -q nest_asyncio
import nest_asyncio
nest_asyncio.apply()


# In[28]:


import json, pandas as pd
from ProjectDomino.modules.Neo4jDataAccess import Neo4jDataAccess
from ProjectDomino.modules.FirehoseJob import FirehoseJob
from ProjectDomino.modules.TwintPool import TwintPool
from prefect.environments.storage import S3
from prefect import Flow,task
from prefect.schedules import IntervalSchedule
from datetime import timedelta, datetime
from random import randrange
from prefect.engine.executors import DaskExecutor
import time
import random


# In[29]:





# In[30]:


S3_BUCKET = "wzy-project-domino"    


# In[31]:


pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


# ## task

# In[33]:


def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def get_creds():
    neo4j_creds = None
    with open('../neo4jcreds.json') as json_file:
        neo4j_creds = json.load(json_file)
    return neo4j_creds

@task(log_stdout=True, skip_on_upstream_skip=True)
def run_stream():
    creds = get_creds()
    start = datetime.strptime("2020-03-11 20:00:00", "%Y-%m-%d %H:%M:%S")
    current = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
    rand_dt=random_date(start, current)
    fh = FirehoseJob(neo4j_creds=creds, save_to_neo=True)
    try:
        for df in fh.search_time_range(Search="covid",Since=str(rand_dt),Until=str(current),job_name="covid stream"):
            logger.debug('got: %s', len(df))
    except:
        logger.debug("job finished")
        


# In[ ]:


schedule = IntervalSchedule(
    start_date=datetime(2020, 9, 5),
    interval=timedelta(seconds=20),
)
storage = S3(bucket=S3_BUCKET)

with Flow("covid stream", storage=storage, schedule=schedule) as flow:
    run_stream()
flow.run()


# In[ ]:



