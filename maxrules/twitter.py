#!/usr/bin/env python
import os
import sys
import argparse
import ConfigParser

from textwrap import TextWrapper
import pymongo
import tweepy

import logging

# CONFIG
max_server_url = 'https://max.upc.edu'
# max_server_url = 'https://sneridagh.upc.es'

twitter_generator_name = 'Twitter'
debug_hashtag = 'debugmaxupcnet'
logging_file = '/var/pyramid/maxserver/var/log/twitter-listener.log'
if not os.path.exists(logging_file):  # pragma: no cover
    logging_file = '/tmp/twitter-listener.log'
logger = logging.getLogger("tweeterlistener")
fh = logging.FileHandler(logging_file, encoding="utf-8")
formatter = logging.Formatter('%(asctime)s %(message)s')
logger.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)


def main(argv=sys.argv, quiet=False):  # pragma: no cover
    # command = MaxTwitterRulesRunnerTest(argv, quiet)
    command = MaxTwitterRulesRunner(argv, quiet)
    return command.run()


class StreamWatcherListener(tweepy.StreamListener):  # pragma: no cover

    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')

    def on_status(self, status):
        try:
            logger.info('Got tweet %d from %s via %s with content: %s' % (status.id, status.author.screen_name, status.source, status.text))
            # Insert the new data in MAX
            from maxrules.tasks import processTweet
            processTweet.delay(status.author.screen_name.lower(), status.text, status.id)
        except:
            # Catch any unicode errors while printing to console
            # and just ignore them to avoid breaking application.
            pass

    def on_error(self, status_code):
        logging.error('An error has occured! Status code = %s' % status_code)
        return True  # keep stream alive

    def on_timeout(self):
        logging.warning('Snoozing Zzzzzz')


class MaxTwitterRulesRunner(object):  # pragma: no cover
    verbosity = 1  # required
    description = "Max rules runner."
    usage = "usage: %prog [options]"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c', '--config',
                      dest='configfile',
                      type=str,
                      help=("Configuration file"))

    def __init__(self, argv, quiet=False):
        self.quiet = quiet
        self.options = self.parser.parse_args()
        config = ConfigParser.ConfigParser()
        config.read(self.options.configfile)

        if not self.options.configfile:
            logging.error('You must provide a valid configuration .ini file.')
            return 2

        try:
            self.consumer_key = config.get('twitter', 'consumer_key')
            self.consumer_secret = config.get('twitter', 'consumer_secret')
            self.access_token = config.get('twitter', 'access_token')
            self.access_token_secret = config.get('twitter', 'access_token_secret')

            self.cluster = config.get('mongodb', 'cluster')
            self.standaloneserver = config.get('mongodb', 'standaloneserver')
            self.clustermembers = config.get('mongodb', 'clustermembers')
            self.dbname = config.get('mongodb', 'dbname')
            self.replicaset = config.get('mongodb', 'replicaset')

        except:
            logging.error('You must provide a valid configuration .ini file.')
            return 2

    def run(self):
        # Querying the BBDD for users to follow - Cluster aware
        if not self.cluster in ['true', 'True', '1', 1]:
            db_uri = self.standaloneserver
            conn = pymongo.MongoClient(db_uri)
        else:
            hosts = self.clustermembers
            replica_set = self.replicaset
            conn = pymongo.MongoReplicaSetClient(hosts, replicaSet=replica_set)

        db = conn[self.dbname]
        contexts_with_twitter_username = db.contexts.find({"twitterUsernameId": {"$exists": True}})
        follow_list = [users_to_follow.get('twitterUsernameId') for users_to_follow in contexts_with_twitter_username]
        contexts_with_twitter_username.rewind()
        readable_follow_list = [users_to_follow.get('twitterUsername') for users_to_follow in contexts_with_twitter_username]

        # Prompt for login credentials and setup stream object
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)

        # auth = tweepy.auth.BasicAuthHandler(self.options.username, self.options.password)
        stream = tweepy.Stream(auth, StreamWatcherListener(), timeout=None)

        # Hardcoded global hashtag(s)
        track_list = ['#upc', '#%s' % debug_hashtag]

        logging.warning("Listening to this Twitter hashtags: %s" % str(track_list))
        logging.warning("Listening to this Twitter userIds: %s" % str(readable_follow_list))

        stream.filter(follow=follow_list, track=track_list)


# For testing purposes only
class MaxTwitterRulesRunnerTest(object):  # pragma: no cover
    verbosity = 1  # required
    description = "Max rules runner."
    usage = "usage: %prog [options]"
    parser = argparse.ArgumentParser(usage, description=description)
    parser.add_argument('-c', '--config',
                      dest='configfile',
                      type=str,
                      help=("Configuration file"))

    def __init__(self, argv, quiet=False):
        self.quiet = quiet
        self.options = self.parser.parse_args()

        logging.warning("Running first time!")

    def run(self):
        while True:
            import time
            time.sleep(2)
            from maxrules.tasks import processTweet
            processTweet('sneridagh', u'Twitejant com un usuari de twitter assignat a un contexte')
            time.sleep(2)
            processTweet('maxupcnet', u'Twitejant amb el hashtag #upc #gsxf')

if __name__ == '__main__':  # pragma: no cover
    try:
        main()
    except KeyboardInterrupt:
        logging.warning('\nGoodbye!')
