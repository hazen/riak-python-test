#!/usr/bin/env python
#include requires pip install python-twitter
import riak
import twitter
import time
import string
import datetime
import argparse
from os import environ
from sys import maxint

def ListTweets(bucket, streaming=False):
    """Dump all keys in the bucket to stdout"""
    decoder = bucket.get_decoder("application/json")
    if streaming:
        allkeys = bucket.stream_keys()
        allkeys = [item for sublist in allkeys for item in sublist]
    else:
        allkeys = bucket.get_keys()
    bucket.allow_mult = True
    count = 0
    if allkeys == None:
        print "No Tweets."
        return
    print "Total keys = {0}".format(len(allkeys))
    for key in allkeys:
        count = count + 1
        obj = bucket.get(key)
        for sibling in obj.siblings:
            json = sibling.encoded_data
            jsdict = decoder(json)
            print "[%d] - %s - %s at %s" % (count,key,jsdict['user'],jsdict['time'])
            print jsdict['tweet'].encode("utf-8")
            links = sibling.links
            for parent_link in links:
                print "LINK"
                (foo,parent_id,baz) = parent_link
                pobj = bucket.get(parent_id)
                for psibling in pobj.siblings:
                    pjson = psibling.encoded_data
                    if pjson != None:
                        jsdict = decoder(pjson)
                        print "\tPARENT - %s at %s" % (jsdict['user'],jsdict['time'])
                        print "\t%s" % jsdict['tweet'].encode("utf-8")
            

def DeleteTweets(bucket):
    """Delete all keys in the bucket"""
    allkeys = bucket.get_keys()
    if allkeys != None:
        count = 0
        for key in allkeys:
            print key
            count = count + 1
            obj = bucket.get(key)
            obj.delete()
            
def LoadTweets(protocol, bucket, quantity, term):
    twitter_consumer_key = environ["TWITTER_CONSUMER_KEY"]
    twitter_consumer_secret = environ["TWITTER_CONSUMER_SECRET"]
    twitter_access_token = environ["TWITTER_ACCESS_TOKEN"]
    twitter_access_token_secret = environ["TWITTER_ACCESS_TOKEN_SECRET"]
    api = twitter.Api(consumer_key=twitter_consumer_key,consumer_secret=twitter_consumer_secret, access_token_key=twitter_access_token, access_token_secret=twitter_access_token_secret)
    
    pages = (quantity+1)/100
    count = 0
    earliest_id = maxint
    for pagenum in range(pages+1):
        #api.GetSearch("DoctorWho", geocode, since_id, earliest_id, until, per_page, page, lang, show_user, result_type, include_entities, query_users)
        perpage = quantity - pagenum * 100
        results = api.GetSearch(term, count=perpage, max_id=earliest_id)
        
        # Make sure search is enabled before storing results
        bucket.enable_search()
        bucket.allow_mult = True
        for status in results:
            if status.id < earliest_id:
                earliest_id = status.id
            # API 1.1 Format: Wed Jun 05 13:02:12 +0000 2013
            # Nuke the timezone cuz datetime.strptime() has some issues
            timestring = status.created_at[:-10] + status.created_at[-4:]
            timestamp = time.strptime(timestring, '%a %b %d %H:%M:%S %Y')
            dt = datetime.datetime(timestamp.tm_year, timestamp.tm_mon, timestamp.tm_mday, timestamp.tm_hour, timestamp.tm_min, timestamp.tm_sec)
            parent = status.in_reply_to_status_id
            count = count + 1
            print "%d = %s - %s at %s" % (count, str(status.id), status.user.screen_name, dt.isoformat())
            print status.text
            tweet = bucket.new(str(status.id), data={                                        
                'tweet': status.text.encode("utf-8"),
                'user': status.user.screen_name,
                'time': dt.isoformat() + 'Z'
                # Add 'Z' for Solr Compatibility
            })
            tweet.add_index('user_bin',status.user.screen_name)
            if parent != None:
                parent_node = bucket.get(str(parent))
                if parent_node:
                    print "LINKING"
                    tweet.add_link(parent_node) 
            tweet.store()
        
def SearchOldTweets(client, bucket, term):
    # First parameter is the bucket we want to search within, the second
    # is the query we want to perform.
    print 'tweet:{0}'.format(term)
    search_query = client.fulltext_search(bucket.name, 'tweet:{0}'.format(term))
    if search_query != None:
        num_found = search_query['num_found']
        if num_found > 0:
            count = 0
            for item in search_query['docs']:
                count = count + 1
                print "%d = %s - %s at %s" % (count, item['id'], item['user'], item['time'])
                print item['tweet']


def SearchTweets(client, bucket, term):
    # First parameter is the bucket we want to search within, the second
    # is the query we want to perform against Riak 2.0 aka Yokozuna

    # Format <key>:*<value>*
    results = bucket.search(term)
    count = 0
    decoder = bucket.get_decoder("application/json")
    for item in results['docs']:
        count = count + 1
        key = item['_yz_rk']
        obj = bucket.get(key)
        for sibling in obj.siblings:
            json = sibling.encoded_data
            jsdict = decoder(json)
            print "[%d] - %s - %s at %s" % (count,key,jsdict['user'],jsdict['time'])
            print jsdict['tweet'].encode("utf-8")


def Search2iTweets(bucket, term):
    decoder = bucket.get_decoder("application/json")
    result = bucket.get_index('user_bin', term)
    if len(result) > 0:
        count = 0
        for key in result:
            count = count + 1
            obj = bucket.get(key)
            json = obj.get_encoded_data()
            jsdict = decoder(json)
            print "[%d] - %s at %s" % (count,jsdict['user'],jsdict['time'])
            print jsdict['tweet']
      
def MapReduceTweets(client, bucket, term):
    query = client.add(bucket.name)
    query.map("function(v) { var data = JSON.parse(v.values[0].data); if(data.user == '%s') { return [[v.key, data]]; } return []; }" % term)
    query.reduce("function(values) { return values.sort(); }")
    
    for result in query.run():
        # Print the key (``v.key``) and the value for that key (``data``).
        print "%s - %s" % (result[0], result[1])

def CreateSearchSchema(client, name):
    xml_file = open(name + '.xml', 'r')
    schema_data = xml_file.read()
    client.create_search_schema(name, schema_data)
    xml_file.close()
    client.create_search_index(name, name)
    time.sleep(5)

    # Associate bucket with search index
    bucket = client.bucket('twitter')
    bucket.set_property('search_index', 'twitter')


# MAIN
parser = argparse.ArgumentParser(description='Brew us some fresh, hot Riak!')
parser.add_argument('-p','--dump', help='Dump all tweets', action='store_true')
parser.add_argument('-l','--load', type=int, help='Load N tweets into Riak')
parser.add_argument('-q','--query', help='Twitter HashTag', default='DoctorWho')
parser.add_argument('-s','--search', help='Search Term')
parser.add_argument('--host', help='Hostname', default='localhost')
parser.add_argument('-t','--http', type=int, help='HTTP port number', default=10018)
parser.add_argument('-b','--pbc', type=int, help='Protocol Buffer port number', default=10017)
parser.add_argument('--protocol', help='Name of transport protocol to use', default='pbc', choices=['http','pbc'])
parser.add_argument('-x','--delete', help='Delete all tweets', action='store_true')
parser.add_argument('-2','--twoi', help='Query 2i')
parser.add_argument('-mr','--mapreduce', help='Test MapReduce to look for a user''s tweets')
parser.add_argument('-sch','--schema', help='Create a YZ search schema from XML file')
args = parser.parse_args()
print args

# Connect to Riak.
options={}
options['timeout'] = 10
if args.protocol != 'pbc':
    client = riak.RiakClient(host=args.host, protocol=args.protocol, http_port=args.http, transport_options = options)
else:
    client = riak.RiakClient(host=args.host, protocol=args.protocol, pb_port=args.pbc, transport_options = options)

# Choose the bucket to store data in.
bucket = client.bucket('twitter')

if args.delete:
    print "Deleting all tweets"
    DeleteTweets(bucket)
elif args.dump:
    print "Dumping all existing tweets in Riak"
    ListTweets(bucket, False)
elif args.twoi != None:
    Search2iTweets(bucket, args.twoi)
elif args.mapreduce != None:
    MapReduceTweets(client, bucket, args.mapreduce)
elif args.load != None:
    print ("Loading %d tweets having term '%s'" % (args.load, args.query))
    LoadTweets(args.protocol, bucket, args.load, args.query)
elif args.search != None:
    print "Searching for term '%s' in loaded tweets" % args.search
    SearchTweets(client, bucket, args.search)
elif args.schema != None:
    print "Creating schema '%s'" % args.schema
    CreateSearchSchema(client, args.schema)
