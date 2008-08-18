from xml import sax

class ClickBankSaxHandler(sax.ContentHandler):
  """Parses through feeds of the sort found at clickbank.com/feeds/marketplace_feed_v1.xml.zip"""
  
  def __init__(self,import_queue,limit=None,upload=True):
    self.limit = limit
    self.upload = upload
    self.import_queue = import_queue
    
    self.categories = []
    self.structure = []
    self.new_category_name = ''
    self.new_site = {}

    self.site_tags = 0
    self.unique_sites = {}
  
  def startElement(self,name,attrs):
    self.structure.append(name)
    
  def endElement(self,name):
    self.structure.pop()

    if name == 'Category':
      self.categories.pop()

    if name == 'Name' and self.structure[-1] == 'Category':
      self.categories.append(self.new_category_name)
      self.new_category_name = ''
      
    if name == 'Site':
      self.new_site['categories'] = self.categories[:]

      if self.upload and not self.new_site['Name'] in self.unique_sites:
        self.import_queue.add(self.new_site)

      self.unique_sites[self.new_site['Name']]= 1
      
      # in case we wanted to limit the number of imports
      self.site_tags += 1
      if self.limit and self.site_tags > self.limit:
        self.endDocument()
        exit()

      self.new_site = {}

  def endDocument(self):
    self.import_queue.finish()
    print "Finished: %d Sites, %d Unique" % (self.site_tags,len(self.unique_sites))

  def characters(self,string):
    if self.structure[-1] == 'Name' and self.structure[-2] == 'Category':
      self.new_category_name += string
      # We had to do this sillyness because category names with html entities in them
      # end up calling the characters method multiple times for the same chunk of cdata
    
    if len(self.structure) >= 2 and self.structure[-2] == 'Site':  
      field = self.structure[-1].encode('ascii')
      
      if field == 'Id':
        self.new_site['Name'] = string
      
      elif field in ['Title','Description']:
        self.new_site[field] = string
        
      elif field in ["Commission"]:
        self.new_site[field] = int(string)
      
      elif field in ["TotalRebillAmt"]:
        self.new_site[field] = int(float(string))
      
      elif field in ["PercentPerSale","Referred"]:
        self.new_site[field] = int(float(string) * 10)
      
      elif field in ["Gravity","EarnedPerSale","TotalEarningsPerSale","TotalRebillAmt"]:
        self.new_site[field] = int(float(string) * 100)
        
      elif field == "PopularityRank":
        self.import_queue.add_popularity(rank=int(string), categories=self.categories[:], site=self.new_site)
      
      # TODO: ADD DATES!
        # rename the ids too
      
  def skippedEntity(self,name):
    "This should never happen"
    print "Skipped: %s" % name
    exit()
    
"""A note on python sillyness:
list[:] creates a shallow copy of a list.  That is, it returns a list with the same elements 
as the original list, but as a unique object.

There is no list.last or list.peek, so list[-1] is used all over.

These are the fields we're looking at in the xml file
tenths - percent_per_sale referred
hundredths - gravity earned_per_sale total_earnings_per_sale total_rebill_amt
publisher nickname = Id, popularity rank, title, description
has recurring products -- not used
"""

import simplejson as json

class BulkImporter(object):  
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
    record = {'Name':site['Name'],'Categories':categories,'Rank':rank,'Type':'popularity'}
    self.popularity_queue.append(record)
    if len(self.popularity_queue) >= self.batch_size*10:
      self._bulk_import_popularity_queue()
            
  def finish(self):
    self._bulk_import_queue()
    self._bulk_import_popularity_queue()
    self._mark_as_imported()
    
    
  def _bulk_import_popularity_queue(self):
    import simplejson as json
    body = '{"docs":%s}' % json.dumps(self.popularity_queue)
    response, content = self.http.request('%s/_bulk_docs' % self.database, 'POST', body=body, headers={"Content-type": "application/json"})
    if response.status != 201:
      print "Popularity: %s:\n%s\n\nOriginal Request Body:\n%s\n" % (response.reason, response, body)
      exit()
    
    # IMPORTANT! Clear the queue!
    self.popularity_queue = []
    
    
    
  def _bulk_import_queue(self):
    
    """
    # Just for indexing.  Of course, these are still teh same objects
    offer_hash = {}
    offer_map = {}
    for item in self.queue:
      name = item['Name']
      item['_id'] = 'offer-%s' % name # TODO: change to include date
      offer_hash[name] = item
      offer_map[name] = 1
    """
    
    #view = """
    #{"map":"function(doc){
    #  hash = %s;
    #  if (hash[doc.Name]) {emit(null, {'_rev':doc._rev,'Name':doc.Name});}
    #}"}
    #""" % json.dumps(offer_map).replace('"',"'")

    #view2 = """
    #{"map":"function(doc){
    #  emit(null,doc.Name)
    #}"}
    #"""

    #print view
    
    """
    headers, response = h.request('%s/_temp_view' % self.database, 'POST',body=view, headers={'Content-Type':'application/json'})
    fetched = json.loads(response)['rows']
    for item in fetched:
      value = item['value']
      revision = value['_rev']
      name = value['Name']
      offer_hash[name]['_rev'] = revision
    """
    # fetch the old ones and update them
    # NOTE: this will be way faster when they finish the bulk fetch command
    # Enable this if you want to see how much slower things go with it.
    """ 
    for name in offer_hash:
      response, content = h.request('%s/offer-%s' % (self.database,name), 'GET', headers={'Accept':'application/json'})
      offer_data = json.loads(content)
      if not 'error' in offer_data:
        # ensures we are updating the latest version
        offer_hash[name]['_rev'] = offer_data['_rev']
        # TODO: add some other updates here!
        # Like, add entries to the histogram    
    """
    
    body = '{"docs":%s}' % json.dumps(self.queue)
    response, content = self.http.request('%s/_bulk_docs' % self.database, 'POST', body=body, headers={"Content-type": "application/json"})
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

def parse_and_upload(path):
  # feeds have a date encoded in their parent directory
  date_str = path.split('/')[-2]
  date = strptime(date_str,"%Y%m%d%H%M%S")

  importer = BulkImporter(date=date,batch=100)
  if importer.is_already_imported():
    print "already imported %s" % importer.date_tag
    return
  else:
    print "importing %s" % importer.date_tag
  
  # Make an Uploading Parser
  handler = ClickBankSaxHandler(importer)
  parser = sax.make_parser()
  parser.setContentHandler(handler)

  # Disable a bunch of annoying features
  parser.setFeature(sax.handler.feature_namespaces,0)
  parser.setFeature(sax.handler.feature_validation,0)
  parser.setFeature(sax.handler.feature_external_ges,0)
  parser.setFeature(sax.handler.feature_external_pes,0)
    
  parser.parse(path)


feed_name = 'marketplace_feed_v1.xml'

# get feeds from commandline arguments
if __name__ == "__main__":
  from sys import argv
  from glob import iglob as glob
  from time import strptime

  for arg in argv[1:]:
    for path in glob("%s/*/%s" % (arg,feed_name)):
      parse_and_upload(path)