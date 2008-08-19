import simplejson as json
from couch_util import headers

class FeedImporter(object):
  def __init__(self,date,batch=100):
    self.batch_size = batch
    self.queue = []
    self.popularity_queue = []

    from httplib2 import Http
    self.http = Http()

    from time import strftime
    self.date_tag = strftime("%Y-%m-%d",date)    

    from settings import database_url
    self.database = database_url

  def add(self,site):
    site['Type'] = 'offer'
    site['Date'] = self.date_tag
    site['_id'] = 'offer-%s-%s' % (site['Name'],self.date_tag)
    self.queue.append(site)
    if len(self.queue) >= self.batch_size:
      self._bulk_import_queue()
      
  def add_popularity(self,rank,categories,site):
    record = {'Name':site['Name'], 'Categories':categories, 'Rank':rank, 'Type':'popularity'}
    self.popularity_queue.append(record)
    if len(self.popularity_queue) >= self.batch_size*10:
      self._bulk_import_popularity_queue()
            
  def finish(self):
    self._bulk_import_queue()
    self._bulk_import_popularity_queue()
    self._mark_as_imported()
    
    
  def _bulk_import_popularity_queue(self):
    body = '{"docs":%s}' % json.dumps(self.popularity_queue)
    
    response, content = self.http.request('%s/_bulk_docs' % self.database, 'POST', body=body, headers=headers.content)
    if response.status != 201:
      print "Popularity: %s:\n%s\n\nOriginal Request Body:\n%s\n" % (response.reason, response, body)
      exit()
    
    # IMPORTANT! Clear the queue!
    self.popularity_queue = []
    
    
  def _bulk_import_queue(self):
    body = '{"docs":%s}' % json.dumps(self.queue)
    response, content = self.http.request('%s/_bulk_docs' % self.database, 'POST', body=body, headers=headers.content)
    if response.status != 201:
      print "debug: offer: %s:\n%s\n\nOriginal Request Body:\n%s\n" % (response.reason, response, body)
      exit()
    
    # IMPORTANT! Clear the queue!
    self.queue = []

  def is_already_imported(self):
    response,content = self.http.request("%s/feed-%s" % (self.database,self.date_tag),'GET')
    if response.status == 404:
      return False
    elif response.status == 200:
      return True
    else:
      print "\ndebug: is_already_imported: %s:\n%s\n\nOriginal Request Body:\n%s\n" % (response.reason, response, body)
      return False
  
  def _mark_as_imported(self):
    from couch_util import report
    feed = {'Type':'feed','Date':self.date_tag,}
    response,content = self.http.request("%s/feed-%s" % (self.database,self.date_tag),'PUT',body=json.dumps(feed),headers={"Content-type": "application/json"})
    if not response.status == 201:
      print "\nFeed: %s\n\n%s" % (response.status,content)
    else:
      print "Success!"


def import_feed(path):
  "feeds must have a date encoded in their parent directory!"
  from time import strptime
  date_str = path.split('/')[-2]
  date = strptime(date_str,"%Y%m%d%H%M%S")

  importer = FeedImporter(date=date,batch=100)
  if importer.is_already_imported():
    print "%s: already imported a feed for %s" % (path, importer.date_tag)
    return
  else:
    print "%s: importing feed for %s" % (path, importer.date_tag)
  
  from clickbank_parser import parse
  parse(path,importer)


def import_dir(path):
  from glob import iglob as glob
  for feed in glob("%s/*/%s" % (path,feed_name)):
    import_feed(feed)


feed_name = 'marketplace_feed_v1.xml'


# commandline arguments give us a directory
if __name__ == "__main__":
  from sys import argv
  for path in argv[1:]:
    import_dir(path)