import pyarrow as pa
import twint
from urlextract import URLExtract
from datetime import datetime, timedelta
import pandas as pd
import json
import logging
import time

logger = logging.getLogger()

extractor = URLExtract()

class TwintPool:

    def __init__(self, is_tor=False):
        self.config = twint.Config()
        self.config.Limit = 1000
        self.config.Pandas = True
        self.config.Hide_output = True
        self.config.Verified = None
        self.config.Username = None
        self.config.User_full = None
        if is_tor:
            self.config.Proxy_host = 'localhost'
            self.config.Proxy_port = "9050"
            self.config.Proxy_type = "socks5"
        else:
            self.config.Proxy_host = None  # "tor"
            self.config.Proxy_port = None  # "9050"
            self.config.Proxy_type = None  # "socks5"

    def twint_loop(self, since, until, stride_sec=600, limit=None):
        def get_unix_time(time_str):
            if isinstance(time_str, datetime):
                return time_str
            return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')

        since = get_unix_time(since)
        until = get_unix_time(until)
        t = since
        tweets_returned = 0

        logger.info('Start twint_loop', extra={'since': since, 'until': until, 'stride_sec': stride_sec, 'limit': limit})
        while t < until and (not tweets_returned or tweets_returned < limit):
            t0 = t
            t1 = t + timedelta(seconds=stride_sec)
            self.config.Since = str(t0)
            self.config.Until = str(t1)
            logger.info('Search step: %s-%s', t0, t1)
            twint.run.Search(self.config)
            tweets_returned += len(twint.storage.panda.Tweets_df)
            if len(twint.storage.panda.Tweets_df) > 0:
                logger.info('Search hit, len %s', len(twint.storage.panda.Tweets_df))
                yield (twint.storage.panda.Tweets_df, t0, t1)
            else:
                logger.debug('not hits on %s - %s, continuing', t0, t1)
            t = t1
        logger.info('twint_loop done, hits: %s', tweets_returned)

    def _get_term(self, Search="IngSoc", Since="1984-04-20 13:00:00", Until="1984-04-20 13:30:00", stride_sec=600,
                  **kwargs):
        tic = time.perf_counter()
        self.config.Search = Search
        self.config.Retweets = True
        for k, v in kwargs.items():
            setattr(self.config, k, v)
        # self.config.Search = term
        logger.info('Start get_term: %s-%s of %s', Search, Since, Until)
        for df, t0, t1 in self.twint_loop(Since, Until, stride_sec, self.config.Limit):
            yield (df, t0, t1)
        toc = time.perf_counter()
        logger.info(f'finished get_term searching for tweets in:  {toc - tic:0.4f} seconds')


    def _get_Replies_to_user(self,username, limit):
        self.config.Retweets = True
        self.config.Search =  username
        self.config.Limit = limit
        self.config.Pandas = True
        self.config.Hide_output = True
        twint.run.Search(self.config)
        return twint.storage.panda.Tweets_df

    def _get_user_timeline(self,username,limit):
        self.config.Profile_full = True
        self.config.Retweets = True
        self.config.Limit = limit
        self.config.Username = username
        self.config.Pandas = True
        self.config.Hide_output = True
        twint.run.Profile(self.config)
        return twint.storage.panda.Tweets_df

    def _get_user_info(self,username):
        self.config.User_full = True
        self.config.Username = username
        self.config.Pandas = True
        self.config.Hide_output=True
        twint.run.Lookup(self.config)
        return twint.storage.panda.User_df

    def check_hydrate(self, df):

        df = df.assign(id=df['id'].astype('int64'))

        df2 = df.drop_duplicates(subset=['id'])
        if len(df2) < len(df):
            logger.warning('Deduplicating input tweet df had duplicates, %s -> %s', len(df), len(df2))
        df = df2

        from .Neo4jDataAccess import Neo4jDataAccess
        neo4j_creds = None
        with open('/secrets/neo4jcreds.json') as json_file:
            neo4j_creds = json.load(json_file)

        # dft : df[[id:int64, hydrated: NaN | 'FULL' | 'PARTIAL'??]]
        dft = Neo4jDataAccess(neo4j_creds=neo4j_creds).get_tweet_hydrated_status_by_id(df)
        logger.debug('hydrate status res sample')
        logger.debug(dft[:3])
        needs_hydrate_ids = dft[dft['hydrated'] != 'FULL'][['id']]
        needs_hydrate_df = df.merge(needs_hydrate_ids, how='inner', on='id')
        logger.info('Hydrate check: df:%s -> dft:%s -> ids:%s => merged:%s', len(df), len(dft), len(needs_hydrate_ids), len(needs_hydrate_df))
        logger.info('First ids: %s', dft['id'][:3])
        #TODO does this include those not in db?

        return needs_hydrate_df

    def twint_df_to_neo4j_df(self, df):
        logger.info('twint_df->neo4j df input: %s', len(df) if not (df is None) else 'None')
        tic = time.perf_counter()
        # df=self.__check_hydrate(df)
        neo4j_df = df.rename(columns={
            'id': 'status_id',
            'tweet': 'full_text',
            'created_at': 'created_at',  # needs to be datetime
            'nlikes': 'favorite_count',
            'nretweets': 'retweet_count',
            # 'user_id_str': 'user_id',
            'username': 'user_name',
            'name': 'user_screen_name'
        })

        def row_to_tweet_type(row):
            #logger.debug('type row: %s', row)
            if row['quote_url'] is None or row['quote_url'] == '':
                return "QUOTE_RETWEET"
            elif ('retweet' in row) and row['retweet']:
                return "RETWEET"
            elif row['id'] == row['conversation_id']:
                return "TWEET"
            elif row['id'] != row['conversation_id']:
                return "REPLY"
            else:
                raise ('wat')

        # def row_to_quoted_status_id(row):
        # if row['quote_url'] and len(row['quote_url']) > 0:
        # return row['quote_url'].split('/')[-1]
        # else:
        # return None

        def row_tweet_to_urls(row):
            return list(extractor.gen_urls(row['tweet']))

        logger.debug('df shape: %s', df.shape)
        logger.debug('cols: %s', df.columns)
        logger.debug('head[3]')
        logger.debug(df[:3])

        neo4j_df['user_location'] = None
        neo4j_df['tweet_type_twint'] = df.apply(row_to_tweet_type, axis=1, result_type='reduce') #handle empty df
        neo4j_df['hashtags'] = df['hashtags'].apply(lambda x: [{'text': ht} for ht in x])
        neo4j_df['user_followers_count'] = None
        neo4j_df['user_friends_count'] = None
        #neo4j_df['date'] = df['date']
        #neo4j_df['user_created_at'] = None
        neo4j_df['user_profile_image_url'] = None
        neo4j_df['reply_tweet_id'] = None
        neo4j_df['user_mentions'] = df['tweet'].str.findall('@[\w]+')
        # neo4j_df['retweet_id'] is suspiciously empty (always)
        neo4j_df['retweeted_status'] = None
        neo4j_df['conversation_id'] = df['conversation_id']  # FIXME no-op?
        neo4j_df['created_at'] = (df['created_at'] / 1000).apply(lambda n: datetime.fromtimestamp(n))

        # neo4j_df['quoted_status_id'] = df.apply(row_to_quoted_status_id, axis=1)
        # neo4j_df['is_quote_status'] = neo4j_df['quoted_status_id'] != None
        neo4j_df['in_reply_to_status_id'] = False
        neo4j_df['urls'] = df.apply(row_tweet_to_urls, axis=1)
        neo4j_df['user_id'] = df['user_id']
        toc = time.perf_counter()
        #logger.debug(f'finished twint to neo prep in:  {toc - tic:0.4f} seconds')
        logger.info('twintdf -> neo4j df output: %s -> %s', len(df), len(neo4j_df) if not (neo4j_df is None) else 'None')
        return neo4j_df


    def to_arrow(self, tweets_df):
        pass





