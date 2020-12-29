import ast
import json
import time
import re

import enum

from datetime import datetime
import pandas as pd
from neo4j import GraphDatabase, basic_auth
from yurl import URL
import logging
from .DfHelper import DfHelper
from .TwintPool import TwintPool

logger = logging.getLogger('Neo4jDataAccess')


class Neo4jDataAccess:
    class NodeLabel(enum.Enum):
        Tweet = 'Tweet'
        Url = 'Url'
        Account = 'Account'

    class RelationshipLabel(enum.Enum):
        TWEETED = 'TWEETED'
        MENTIONED = 'MENTIONED'
        QUOTED = 'QUOTED'
        REPLIED = 'REPLIED'
        RETWEETED = 'RETWEETED'
        INCLUDES = 'INCLUDES'

    class RoleType(enum.Enum):
        READER = 'reader'
        WRITER = 'writer'

    def __init__(self, debug=False, neo4j_creds=None, batch_size=2000, timeout="60s"):
        self.creds = neo4j_creds
        self.debug = debug
        self.timeout = timeout
        self.batch_size = batch_size
        self.tweetsandaccounts = """
                  UNWIND $tweets AS t
                      //Add the Tweet
                    MERGE (tweet:Tweet {id:t.tweet_id})
                        ON CREATE SET
                            tweet.text = t.text,
                            tweet.created_at = t.tweet_created_at,
                            tweet.favorite_count = t.favorite_count,
                            tweet.retweet_count = t.retweet_count,
                            tweet.record_created_at = timestamp(),
                            tweet.job_name = t.job_name,
                            tweet.job_id = t.job_id,
                            tweet.hashtags = t.hashtags,
                            tweet.hydrated = 'FULL',
                            tweet.type = t.tweet_type
                        ON MATCH SET
                            tweet.text = t.text,
                            tweet.favorite_count = t.favorite_count,
                            tweet.retweet_count = t.retweet_count,
                            tweet.record_updated_at = timestamp(),
                            tweet.job_name = t.job_name,
                            tweet.job_id = t.job_id,
                            tweet.hashtags = t.hashtags,
                            tweet.hydrated = 'FULL',
                            tweet.type = t.tweet_type
                    //Add Account
                    MERGE (user:Account {id:t.user_id})
                        ON CREATE SET
                            user.id = t.user_id,
                            user.name = t.name,
                            user.screen_name = t.user_screen_name,
                            user.followers_count = t.user_followers_count,
                            user.friends_count = t.user_friends_count,
                            user.location = t.user_location,
                            user.user_profile_image_url = t.user_profile_image_url,
                            user.created_at = t.user_created_at,
                            user.record_created_at = timestamp(),
                            user.job_name = t.job_name,
                            user.hydrated = 'FULL',
                            user.job_id = t.job_id
                        ON MATCH SET
                            user.name = t.user_name,
                            user.screen_name = t.user_screen_name,
                            user.followers_count = t.user_followers_count,
                            user.friends_count = t.user_friends_count,
                            user.user_profile_image_url = t.user_profile_image_url,
                            user.location = t.user_location,
                            user.created_at = t.user_created_at,
                            user.record_updated_at = timestamp(),
                            user.job_name = t.job_name,
                            user.hydrated = 'FULL',
                            user.job_id = t.job_id
                    //Add Reply to tweets if needed
                    FOREACH(ignoreMe IN CASE WHEN t.tweet_type='REPLY' THEN [1] ELSE [] END |
                        MERGE (retweet:Tweet {id:t.reply_tweet_id})
                            ON CREATE SET retweet.id=t.reply_tweet_id,
                            retweet.record_created_at = timestamp(),
                            retweet.job_name = t.job_name,
                            retweet.job_id = t.job_id,
                            retweet.hydrated = 'PARTIAL'
                    )
                    //Add QUOTE_RETWEET to tweets if needed
                    FOREACH(ignoreMe IN CASE WHEN t.tweet_type='QUOTE_RETWEET' THEN [1] ELSE [] END |
                        MERGE (quoteTweet:Tweet {id:t.quoted_status_id})
                            ON CREATE SET quoteTweet.id=t.quoted_status_id,
                            quoteTweet.record_created_at = timestamp(),
                            quoteTweet.job_name = t.job_name,
                            quoteTweet.job_id = t.job_id,
                            quoteTweet.hydrated = 'PARTIAL'
                    )
                    //Add RETWEET to tweets if needed
                    FOREACH(ignoreMe IN CASE WHEN t.tweet_type='RETWEET' THEN [1] ELSE [] END |
                        MERGE (retweet:Tweet {id:t.retweet_id})
                            ON CREATE SET retweet.id=t.retweet_id,
                            retweet.record_created_at = timestamp(),
                            retweet.job_name = t.job_name,
                            retweet.job_id = t.job_id,
                            retweet.hydrated = 'PARTIAL'
                    )
        """

        self.tweeted_rel = """UNWIND $tweets AS t
                    MATCH (user:Account {id:t.user_id})
                    MATCH (tweet:Tweet {id:t.tweet_id})
                    OPTIONAL MATCH (replied:Tweet {id:t.reply_tweet_id})
                    OPTIONAL MATCH (quoteTweet:Tweet {id:t.quoted_status_id})
                    OPTIONAL MATCH (retweet:Tweet {id:t.retweet_id})
                    WITH user, tweet, replied, quoteTweet, retweet
                    MERGE (user)-[r:TWEETED]->(tweet)
                    FOREACH(ignoreMe IN CASE WHEN tweet.type='REPLY' AND replied.id>0 THEN [1] ELSE [] END |
                        MERGE (tweet)-[:REPLYED]->(replied)
                    )
                    FOREACH(ignoreMe IN CASE WHEN tweet.type='QUOTE_RETWEET' AND quoteTweet.id>0 THEN [1] ELSE [] END |
                        MERGE (tweet)-[:QUOTED]->(quoteTweet)
                    )
                    FOREACH(ignoreMe IN CASE WHEN tweet.type='RETWEET' AND retweet.id>0 THEN [1] ELSE [] END |
                        MERGE (tweet)-[:RETWEETED]->(retweet)
                    )
        """

        self.mentions = """UNWIND $mentions AS t
                    MATCH (tweet:Tweet {id:t.tweet_id})
                    MERGE (user:Account {id:t.user_id})
                        ON CREATE SET
                            user.id = t.user_id,
                            user.mentioned_name = t.name,
                            user.mentioned_screen_name = t.user_screen_name,
                            user.record_created_at = timestamp(),
                            user.job_name = t.job_name,
                            user.hydrated = 'PARTIAL',
                            user.job_id = t.job_id
                    WITH user, tweet
                    MERGE (tweet)-[:MENTIONED]->(user)
        """

        self.urls = """UNWIND $urls AS t 
                    MATCH (tweet:Tweet {id:t.tweet_id})
                    MERGE (url:Url {full_url:t.url})
                        ON CREATE SET
                            url.full_url = t.url,
                            url.job_name = t.job_name,
                            url.job_id = t.job_id,
                            url.record_created_at = timestamp(),
                            url.schema=t.scheme,
                            url.netloc=t.netloc,
                            url.path=t.path,
                            url.params=t.params,
                            url.query=t.query,
                            url.fragment=t.fragment,
                            url.username=t.username,
                            url.password=t.password,
                            url.hostname=t.hostname,
                            url.port=t.port
                    WITH url, tweet
                    MERGE (tweet)-[:INCLUDES]->(url)
        """

        self.fetch_tweet_status = """UNWIND $ids AS i
                    MATCH (tweet:Tweet {id:i.id})
                    RETURN tweet.id, tweet.hydrated
        """

        self.fetch_tweet = """UNWIND $ids AS i
                    MATCH (tweet:Tweet {id:i.id})
                    RETURN tweet
        """

        self.fetch_account_status = """UNWIND $ids AS i
                    MATCH (user:Account {id:i.id})
                    RETURN user.id, user.hydrated
        """

    def __get_neo4j_graph(self, role_type):
        creds = None
        logging.debug('role_type: %s', role_type)
        if not (self.creds is None):
            creds = self.creds
        else:
            with open('/secrets/neo4jcreds.json') as json_file:
                creds = json.load(json_file)
        res = list(filter(lambda c: c["type"] == role_type, creds))
        if len(res):
            logging.debug("creds %s", res)
            creds = res[0]["creds"]
            uri = f'bolt://{creds["host"]}:{creds["port"]}'
            self.graph = GraphDatabase.driver(
                uri, auth=basic_auth(creds['user'], creds['password']), encrypted=False)
        else:
            self.graph = None
        return self.graph

    def get_neo4j_graph(self, role_type: RoleType):
        if not isinstance(role_type, self.RoleType):
            raise TypeError('The role_type parameter is not of type RoleType')
        return self.__get_neo4j_graph(role_type.value)

    def get_from_neo(self, cypher: str, limit=1000):
        graph = self.__get_neo4j_graph('reader')
        # If the limit isn't set in the traversal, and it isn't None, then add it
        if limit and not re.search('LIMIT', cypher, re.IGNORECASE):
            cypher = cypher + " LIMIT " + str(limit)
        with graph.session() as session:
            result = session.run(cypher, timeout=self.timeout)
            df = pd.DataFrame([dict(record) for record in result])
        if not limit:
            return df
        else:
            return df.head(limit)

    def get_tweet_by_id(self, df: pd.DataFrame, cols=[]):
        if 'id' in df:
            graph = self.__get_neo4j_graph('reader')
            ids = df[['id']].assign(id=df['id'].astype('int64')).to_dict(orient='records')
            with graph.session() as session:
                result = session.run(self.fetch_tweet, ids=ids, timeout=self.timeout)
                res = pd.DataFrame([dict(record) for record in result])
            logging.debug('Response info: %s rows, %s columns: %s' %
                          (len(res), len(res.columns), res.columns))
            pdf = pd.DataFrame()
            for r in res.iterrows():
                props = {}
                for k in r[1]['tweet'].keys():
                    if cols:
                        if k in cols:
                            props.update({k: r[1]['tweet'][k]})
                    else:
                        props.update({k: r[1]['tweet'][k]})
                pdf = pdf.append(props, ignore_index=True)
            return pdf
        else:
            raise TypeError(
                'Parameter df must be a DataFrame with a column named "id" ')

    def save_enrichment_df_to_graph(self, label: NodeLabel, df: pd.DataFrame, job_name: str, job_id=None):
        if not isinstance(label, self.NodeLabel):
            raise TypeError('The label parameter is not of type NodeType')

        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                'The df parameter is not of type Pandas.DataFrame')

        idColName = 'full_url' if label == self.NodeLabel.Url else 'id'
        statement = 'UNWIND $rows AS t'
        statement += '   MERGE (n:' + label.value + \
                     ' {' + idColName + ':t.' + idColName + '}) ' + \
                     ' SET '

        props = []
        for column in df:
            if not column == idColName:
                props.append(f' n.{column} = t.{column} ')

        statement += ','.join(props)
        graph = self.__get_neo4j_graph('writer')
        with graph.session() as session:
            result = session.run(
                statement, rows=df.to_dict(orient='records'), timeout=self.timeout)

    def save_parquet_df_to_graph(self, df: pd.DataFrame, job_name: str, job_id=None):
        pdf = DfHelper().normalize_parquet_dataframe(df)
        logging.info('Saving to Neo4j')
        self.__save_df_to_graph(pdf, job_name)

    # Get the status of a DataFrame of Tweets by id.  Returns a dataframe with the hydrated status
    def get_tweet_hydrated_status_by_id(self, df: pd.DataFrame):
        df = df.assign(id=df['id'].astype('int64'))
        if 'id' in df:
            graph = self.__get_neo4j_graph('reader')
            ids = df[['id']].assign(id=df['id'].astype('int64')).to_dict(orient='records')
            with graph.session() as session:
                result = session.run(self.fetch_tweet_status, ids=ids)
                res = pd.DataFrame([dict(record) for record in result])
                # res['tweet.id'] = res['tweet.id'].astype('int64')
            logging.debug('Response info: %s rows, %s columns: %s' %
                          (len(res), len(res.columns), res.columns))
            if len(res) == 0:
                return df[['id']].assign(hydrated=None)
            else:
                res = res.rename(
                    columns={'tweet.id': 'id', 'tweet.hydrated': 'hydrated'})
                # ensures hydrated=None if Neo4j does not answer for id
                res = df[['id']].merge(res, how='left', on='id')
                return res
        else:
            logging.debug('df columns %s', df.columns)
            raise Exception(
                'Parameter df must be a DataFrame with a column named "id" ')

    # Get the status of a DataFrame of Tweets by id.  Returns a dataframe with the hydrated status

    # Get the status of a DataFrame of Account by id.  Returns a dataframe with the hydrated status
    def get_account_hydrated_status_by_id(self, df: pd.DataFrame):
        if 'id' in df:
            graph = self.__get_neo4j_graph('reader')
            ids = df[['id']].assign(id=df['id'].astype('int64')).to_dict(orient='records')
            with graph.session() as session:
                result = session.run(self.fetch_account_status, ids=ids)
                res = pd.DataFrame([dict(record) for record in result])
            logging.debug('Response info: %s rows, %s columns: %s' %
                          (len(res), len(res.columns), res.columns))
            if len(res) == 0:
                return df[['id']].assign(hydrated=None)
            else:
                res = res.rename(
                    columns={'user.id': 'id', 'user.hydrated': 'hydrated'})
                # ensures hydrated=None if Neo4j does not answer for id
                res = df[['id']].merge(res, how='left', on='id')
                return res
        else:
            logging.debug('df columns %s', df.columns)
            raise Exception(
                'Parameter df must be a DataFrame with a column named "id" ')

    # This saves the User and Tweet data right now
    def __save_df_to_graph(self, df, job_name, job_id=None):
        graph = self.__get_neo4j_graph('writer')
        global_tic = time.perf_counter()
        params = []
        mention_params = []
        url_params = []
        tic = time.perf_counter()
        logging.debug('df columns %s', df.columns)
        for index, row in df.iterrows():
            # determine the type of tweet
            tweet_type = 'TWEET'
            if row['tweet_type_twint']:
                tweet_type = row['tweet_type_twint']
            elif row["in_reply_to_status_id"] is not None and row["in_reply_to_status_id"] > 0:
                tweet_type = "REPLY"
            elif "quoted_status_id" in row and row["quoted_status_id"] is not None and row["quoted_status_id"] > 0:
                tweet_type = "QUOTE_RETWEET"
            elif "retweet_id" in row and row["retweet_id"] is not None and row["retweet_id"] > 0:
                tweet_type = "RETWEET"
            try:
                params.append({'tweet_id': row['status_id'],
                               'text': row['full_text'],
                               'tweet_created_at': row['created_at'].to_pydatetime(),
                               'favorite_count': row['favorite_count'],
                               'retweet_count': row['retweet_count'],
                               'tweet_type': tweet_type,
                               'job_id': job_id,
                               'job_name': job_name,
                               'hashtags': self.__normalize_hashtags(row['hashtags']),
                               'user_id': row['user_id'],
                               'user_name': row['user_name'],
                               'user_location': row['user_location'],
                               'user_screen_name': row['user_screen_name'],
                               'user_followers_count': row['user_followers_count'],
                               'user_friends_count': row['user_friends_count'],
                               'user_created_at': pd.Timestamp(row['user_created_at'], unit='s').to_pydatetime(),
                               'user_profile_image_url': row['user_profile_image_url'],
                               'reply_tweet_id': row['in_reply_to_status_id'],
                               'conversation_id': row['conversation_id'] if 'conversation_id' in row else None,
                               'quoted_status_id': row['quoted_status_id'],
                               'retweet_id': row['retweet_id'] if 'retweet_id' in row else None,
                               'geo': row['geo'] if 'geo' in row else None,
                               'ingest_method': row['ingest_method']
                               })
            except Exception as e:
                logging.error('params.append exn', e)
                logging.error('row', row)
                raise e

            # if there are urls then populate the url_params
            if row['urls']:
                url_params = self.__parse_urls(
                    row, url_params, job_name, job_id)
            # if there are user_mentions then populate the mentions_params
            if row['user_mentions']:
                for m in row['user_mentions']:
                    mention_params.append({
                        'tweet_id': row['status_id'],
                        'user_id': m['id'],
                        'user_name': m['name'],
                        'user_screen_name': m['screen_name'],
                        'job_id': job_id,
                        'job_name': job_name,
                    })
            if index % self.batch_size == 0 and index > 0:
                self.__write_to_neo(params, url_params, mention_params)
                toc = time.perf_counter()
                logging.info(
                    f'Neo4j Periodic Save Complete in  {toc - tic:0.4f} seconds')
                params = []
                mention_params = []
                url_params = []
                tic = time.perf_counter()

        self.__write_to_neo(params, url_params, mention_params)
        toc = time.perf_counter()
        logging.info(
            f"Neo4j Import Complete in  {toc - global_tic:0.4f} seconds")

    def __write_to_neo(self, params, url_params, mention_params):
        try:
            with self.graph.session() as session:
                session.run(self.tweetsandaccounts,
                            tweets=params, timeout=self.timeout)
                session.run(self.tweeted_rel, tweets=params,
                            timeout=self.timeout)
                session.run(self.mentions, mentions=mention_params,
                            timeout=self.timeout)
                session.run(self.urls, urls=url_params, timeout=self.timeout)
        except Exception as inst:
            logging.error('Neo4j Transaction error')
            logging.error(type(inst))  # the exception instance
            logging.error(inst.args)  # arguments stored in .args
            # __str__ allows args to be printed directly,
            logging.error(inst)
            raise inst

    def __normalize_hashtags(self, value):
        if value:
            hashtags = []
            for h in value:
                hashtags.append(h['text'])
            return ','.join(hashtags)
        else:
            return None

    def __parse_urls(self, row, url_params, job_name, job_id=None):
        for u in row['urls']:
            try:
                parsed = URL(u['expanded_url'])
                url_params.append({
                    'tweet_id': row['status_id'],
                    'url': u['expanded_url'],
                    'job_id': job_id,
                    'job_name': job_name,
                    'schema': parsed.scheme,
                    'netloc': parsed.authority,
                    'path': parsed.path,
                    'params': '',#parsed.params,
                    'query': parsed.query,
                    'fragment': parsed.fragment,
                    'username': parsed.username,
                    'password': parsed.authorization,
                    'hostname': parsed.host,
                    'port': parsed.port,
                })
            except Exception as inst:
                logging.error(type(inst))  # the exception instance
                logging.error(inst.args)  # arguments stored in .args
                # __str__ allows args to be printed directly,
                logging.error(inst)
        return url_params

    def __enrich_usr_info(self, df):
        # df=df.drop_duplicates(subset=["id"])
        lst = [TwintPool()._get_user_info(username=user) for user in df["user_name"].to_list()]
        dfs = pd.concat(lst).drop_duplicates(subset=["id"])
        return dfs

    def enrich_user_tl_and_info(self,username, job_name,job_id, limit, include_profile_fetch=False):
        if include_profile_fetch:
            tic = time.perf_counter()
            chk = TwintPool().twint_df_to_neo4j_df(TwintPool()._get_user_timeline(username=username, limit=limit))
            usr_df = self.__tweetdf_to_neo_account_df(TwintPool()._get_user_info(username=username), job_name=job_name)
            chk['tmp'] = 1
            usr_df['tmp'] = 1
            df = pd.merge(chk, usr_df, on=['tmp'])
            df = df.drop('tmp', axis=1)
            res = {"params": df}
            toc = time.perf_counter()
            logger.info(f'finished account and data enrichments in:  {toc - tic:0.4f} seconds writing to neo4j now..')
            self.write_twint_enriched_tweetdf_to_neo(res, job_name, job_id)
        else:
            tic = time.perf_counter()
            chk = TwintPool().twint_df_to_neo4j_df(TwintPool()._get_user_timeline(username=username, limit=limit))
            res = {"params": chk}
            toc = time.perf_counter()
            logger.info(f'finished account and data enrichments in:  {toc - tic:0.4f} seconds writing to neo4j now..')
            self.write_twint_enriched_tweetdf_to_neo(res, job_name, job_id)

    def save_twintdf_to_neo(self, df, job_name, job_id=None):
        if (df is None) or (len(df) == 0):
            logger.info('Empty df for neo conversion, skip')
            return None

        twintdftic = time.perf_counter()
        df = TwintPool().twint_df_to_neo4j_df(df)
        twintdftoc = time.perf_counter()
        logger.info('twintdf->neo4jdf: %s', len(df) if not (df is None) else 'None')
        logger.info(f'finished twint dataframe to neo4j conversion stage 1 {twintdftoc - twintdftic:0.4f} seconds')

        df.drop(df.columns[df.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)
        # df=df.stack().droplevel(level=0)
        global_tic = time.perf_counter()
        params = []
        tic = time.perf_counter()
        logger.debug('df columns %s', df.columns)

        itertic = time.perf_counter()
        for index, row in df.iterrows():
            # determine the type of tweet
            tweet_type = 'TWEET'
            if row['tweet_type_twint']:
                tweet_type = row['tweet_type_twint']
            elif row["in_reply_to_status_id"] is not None and row["in_reply_to_status_id"] > 0:
                tweet_type = "REPLY"
            elif "quoted_status_id" in row and row["quoted_status_id"] is not None and row["quoted_status_id"] > 0:
                tweet_type = "QUOTE_RETWEET"
            elif "retweet_id" in row and row["retweet_id"] is not None and row["retweet_id"] > 0:
                tweet_type = "RETWEET"
            try:
                params.append(pd.DataFrame([{'tweet_id': int(row['status_id']),
                                             'text': row['full_text'],
                                             'created_at': str(pd.to_datetime(row['created_at'])),
                                             'favorite_count': row['favorite_count'],
                                             'retweet_count': row['retweet_count'],
                                             'type': tweet_type,
                                             'job_id': job_id,
                                             'job_name': job_name,
                                             'hashtags': self.__normalize_hashtags(row['hashtags']),
                                             'user_id': row['user_id'],
                                             'user_name': row['user_name'],
                                             'user_location': row['user_location'] if 'user_location' in row else None,
                                             'user_screen_name': row['user_screen_name'],
                                             'user_followers_count': row[
                                                 'user_followers_count'] if 'user_followers_count' in row else None,
                                             'user_friends_count': row[
                                                 'user_friends_count'] if 'user_friends_count' in row else None,
                                             'user_profile_image_url': row[
                                                 'user_profile_image_url'] if 'user_profile_image_url' in row else None,
                                             'reply_tweet_id': row[
                                                 'in_reply_to_status_id'] if 'in_reply_to_status_id' in row else None,
                                             'conversation_id': row[
                                                 'conversation_id'] if 'conversation_id' in row else None,
                                             'quoted_status_id': row[
                                                 'quoted_status_id'] if 'quoted_status_id' in row else None,
                                             'retweet_id': row['retweet_id'] if 'retweet_id' in row else None,
                                             'geo': row['geo'] if 'geo' in row else None,
                                             }]))

            except Exception as e:
                logger.error('params.append exn', e)
                logger.error('row', row)
                raise e
        itertoc = time.perf_counter()
        logger.info(f'finished iterating twint df and determining tweet type  {itertoc - itertic:0.4f} seconds')


        urltic = time.perf_counter()
        url_df = self.__urldf_to_neodf(self.__parse_urls_twint(df, job_name, job_id))
        urltoc = time.perf_counter()
        logger.info(f'finished parsing urls in:  {urltoc - urltic:0.4f} seconds')


        #accttic = time.perf_counter()
        #acct_df = self.__tweetdf_to_neo_account_df(
            #self.__enrich_usr_info(pd.concat(params, ignore_index=True, sort=False)), job_name)
        #accttoc = time.perf_counter()
        #logger.debug(f'finished user enrichment in:  {accttoc - accttic:0.4f} seconds')


        menttic = time.perf_counter()
        mention_df = self.__parse_mentions_twint(df, job_name, job_id)
        menttoc = time.perf_counter()
        logger.info(f'finished parsing mentions in:  {menttoc - menttic:0.4f} seconds')

        #paramstic = time.perf_counter()
        params_df = pd.concat(params, ignore_index=True, sort=False)
        params_df["hydrated"] = "PARTIAL"
        #params_df = pd.concat([params_df,acct_df], axis=1, ignore_index=False, sort=False)
        #paramstoc = time.perf_counter()
        #logger.debug(f'finished correlating account info to tweets in:  {paramstoc - paramstic:0.4f} seconds')
        logger.info('split tweets (%s) into mentions (%s), urls (%s), and tweets (%s)', len(df), len(mention_df), len(url_df), len(params_df))

        # if df.index.all() % self.batch_size == 0 and df.index.all() > 0:
        res = {"mentions": mention_df, "urls": url_df, "params": params_df}
        toc = time.perf_counter()
        logger.info(f'finished data enrichments in:  {toc - tic:0.4f} seconds writing to neo4j now..')
        self.write_twint_enriched_tweetdf_to_neo(res, job_name, job_id)
        #return res

    def __normalize_hashtags(self, value):
        if value:
            hashtags = []
            for h in value:
                hashtags.append(h['text'])
            return ','.join(hashtags)
        else:
            return None

    def __parse_urls_twint(self, df, job_name, job_id):
        counter = 0
        url_params_lst = []
        try:
            for index, row in df.iterrows():
                if row["urls"]:
                    urls = [url for url in row["urls"]]
                    parsed = URL(urls[counter])
                    url_params_lst.append(pd.DataFrame([{
                        'tweet_id': int(row["status_id"]),
                        'full_url': urls[counter],
                        'job_id': job_id,
                        'job_name': job_name,
                        'schema': parsed.scheme,
                        'netloc': parsed.authority,
                        'path': parsed.path,
                        'params': '',#parsed.params,
                        'query': parsed.query,
                        'fragment': parsed.fragment,
                        'username': parsed.username,
                        'password': parsed.authorization,
                        'hostname': parsed.host,
                        'port': parsed.port}]))
        except Exception as e:
            logging.error('params.append exn', e)
            logging.error('row', row)
            raise e
        if len(url_params_lst) == 0:
            return pd.DataFrame({
                'tweet_id': pd.Series([], dtype='int64'),
                'full_url': pd.Series([], dtype='object'),
                'job_id': pd.Series([], dtype='object'),
                'job_name': pd.Series([], dtype='object'),
                'schema': pd.Series([], dtype='object'),
                'netloc': pd.Series([], dtype='object'),
                'path': pd.Series([], dtype='object'),
                'params': pd.Series([], dtype='object'),
                'query': pd.Series([], dtype='object'),
                'fragment': pd.Series([], dtype='object'),
                'username': pd.Series([], dtype='object'),
                'password': pd.Series([], dtype='object'),
                'hostname': pd.Series([], dtype='object'),
                'port': pd.Series([], dtype='int64')
            })
        url_df = pd.concat(url_params_lst, ignore_index=True, sort=False)
        counter += 1
        return url_df

    def __parse_mentions_twint(self, df, job_name, job_id=None):
        counter = 0
        mention_lst = []
        mentions = [x for x in df['user_mentions'].to_list()]
        ids = [i for i in df["status_id"].to_list()]
        for m in mentions:
            mention_lst.append(pd.DataFrame([{
                'tweet_id': ids[counter],
                'user_screen_name': m,
                'job_id': job_id,
                'job_name': job_name,
            }]))
            counter += 1
        if len(mention_lst) == 0:
            return pd.DataFrame({
                'tweet_id': pd.Series([], dtype='int64'),
                'user_screen_name': pd.Series([], dtype='object'),
                'job_id': pd.Series([], dtype='object'),
                'job_name': pd.Series([], dtype='object')
            })
        mention_df = pd.concat(mention_lst, ignore_index=True, sort=False)
        return mention_df

    def __urldf_to_neodf(self, df):
        neourldf = df[['tweet_id', 'full_url', 'job_name', 'schema', 'netloc',
                       'path', 'params', 'query', 'fragment', 'username',
                       'password', 'hostname', 'port']]
        neourldf['record_created_at'] = str(datetime.now())
        return neourldf

    def __tweetdf_to_neo_account_df(self, df, job_name):
        acctdf = df[["location", "name"]]
        acctdf['record_created_at'] = str(datetime.now())
        acctdf['screen_name'] = df['username']
        acctdf['friends_count'] = df["following"]
        acctdf['followers_count'] = df["followers"]
        #acctdf['user_created_at'].apply(lambda n: str(pd.to_datetime(n)))
        acctdf['user_created_at'].apply(lambda n: pd.Timestamp(n, unit='s').to_pydatetime())
        acctdf['job_name'] = str(job_name)
        return acctdf


    def write_twint_enriched_tweetdf_to_neo(self, res, job_name, job_id):
        graph = self.__get_neo4j_graph('writer')
        global_tic = time.perf_counter()
        tic = time.perf_counter()
        key = None
        df = None
        try:
            for key in list(res.keys()):
                df = res[key]
                # if df.index.all() % self.batch_size == 0 and df.index.all() > 0:
                if key == 'mentions':
                    logger.info("writing mentions (%s)", len(df))
                    with graph.session() as session:
                        df_with_mentions = df[ df['user_screen_name'].apply(len) > 0 ]
                        session.run(self.mentions, mentions=df_with_mentions.to_dict(orient='records'), timeout=self.timeout)
                elif key == 'urls':
                    logger.info("writing URL Nodes and Properties (%s)", len(df))
                    self.save_enrichment_df_to_graph(self.NodeLabel.Url, df, job_name, job_id)
                elif key == 'params':
                    logger.info("writing tweets and accts (%s)", len(df))
                    with self.graph.session() as session:
                        logger.info('writing tweets and accounts')
                        session.run(self.tweetsandaccounts, tweets=df.to_dict(orient='records'),
                                    timeout=self.timeout)
                        logger.info('writing tweet relationships')
                        session.run(self.tweeted_rel, tweets=df.to_dict(orient='records'), timeout=self.timeout)
            toc = time.perf_counter()
            logger.info(f'Neo4j Periodic Save Complete in  {toc - tic:0.4f} seconds')
        except Exception as inst:
            logging.error('//////////////')
            logging.error('Neo4j Transaction error', exc_info=True)
            logging.error(type(inst))  # the exception instance
            logging.error(inst.args)  # arguments stored in .args
            # __str__ allows args to be printed directly,
            logging.error(inst)
            logging.error('--------------')
            logging.error('KEY: %s', key)
            if key == 'mentions':
                logger.error('MENTIONS: %s', self.mentions)
                logger.error('df_with_mentions: %s', df_with_mentions)
            elif key == 'urls':
                logger.error('URL: %s', self.NodeLabel.Url)
            elif key == 'params':
                logger.error('tweetsandaccounts: %s', self.tweetsandaccounts)
                logger.error('tweeted_rel: %s', self.tweeted_rel)
            logging.error('df: %s', df)
            logging.error('//////////////')
            raise inst






