from os import path
from sys import argv
from xml import sax

class ClickBankSaxHandler(sax.ContentHandler):
  """Parses through feeds of the sort found at clickbank.com/feeds/marketplace_feed_v1.xml.zip"""
  
  def __init__(self,import_queue):
    self.categories = []
    self.structure = []
    self.new_category_name = ''
    self.new_site = {}
    self.counter = 0
    self.import_queue = import_queue
    self.sites_found = {}
  
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

      if not self.new_site['Name'] in self.sites_found:
        self.import_queue.add(self.new_site)
      self.sites_found[self.new_site['Name']]= 1
      
      # in case we wanted to limit the number of imports
      self.counter += 1
      #if self.counter > 10000:
      #  self.endDocument()
      #  exit()

      self.new_site = {}

  def endDocument(self):
    self.import_queue.finish()
    print "There are %d offers" % self.counter
    print "%d are unique" % len(self.sites_found)

  def characters(self,string):
    if self.structure[-1] == 'Name' and self.structure[-2] == 'Category':
      self.new_category_name += string
      # We had to do this sillyness because category names with html entities in them
      # end up calling the characters method multiple times for the same chunk of cdata
    
    if len(self.structure) >= 2 and self.structure[-2] == 'Site':  
      field = self.structure[-1].encode('ascii')
      
      if field == 'Id':
        self.new_site['Name'] = string
        
      if field in ['Title','Description']:
        self.new_site[field] = string
        
      if field == "PopularityRank":
        self.new_site[field] = int(string)
        
      if field in ["PercentPerSale","Referred"]:
        self.new_site[field] = int(float(string) * 10) # NOTE: why are we doing this?
      
      if field in ["Gravity","EarnedPerSale","TotalEarningsPerSale","TotalRebillAmt"]:
        self.new_site[field] = int(float(string) * 100) # NOTE: seriously, these numbers seem messed up.
      
  def skippedEntity(self,name):
    "This should never happen"
    print "Skipped: %s" % name
    
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

class BulkImporter(object):
  
  def __init__(self,batch_size=100):
    self.batch_size = batch_size
    self.queue = []

  def add(self,site):
    self.queue.append(site)
    if len(self.queue) >= self.batch_size:
      self._bulk_import_queue()
      
  def finish(self):
    self._bulk_import_queue()
    
  def _bulk_import_queue(self):
    from settings import database_url
    from httplib2 import Http
    import simplejson as json
    h = Http()
    
    # Just for indexing.  Of course, these are still teh same objects
    offer_hash = {}
    offer_map = {}
    for item in self.queue:
      name = item['Name']
      item['_id'] = 'offer-%s' % name
      offer_hash[name] = item
      offer_map[name] = 1

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
    headers, response = h.request('%s/_temp_view' % database_url, 'POST',body=view, headers={'Content-Type':'application/json'})
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
      headers, response = h.request('%s/offer-%s' % (database_url,name), 'GET', headers={'Accept':'application/json'})
      offer_data = json.loads(response)
      if not 'error' in offer_data:
        # ensures we are updating the latest version
        offer_hash[name]['_rev'] = offer_data['_rev']
        # TODO: add some other updates here!
        # Like, add entries to the histogram    
    """

    body = '{"docs":%s}' % json.dumps(self.queue)
    headers, response = h.request('%s/_bulk_docs' % database_url, 'POST', body=body, headers={"Content-type": "application/json"})
    self.queue = []


def make_uploading_parser():
  # Make a Parser
  batch_size = 100
  importer = BulkImporter(100)
  handler = ClickBankSaxHandler(importer)
  parser = sax.make_parser()
  parser.setContentHandler(handler)

  # Disable a bunch of annoying features
  parser.setFeature(sax.handler.feature_namespaces,0)
  parser.setFeature(sax.handler.feature_validation,0)
  parser.setFeature(sax.handler.feature_external_ges,0)
  parser.setFeature(sax.handler.feature_external_pes,0)
  
  return parser

# get feeds from commandline arguments
if __name__ == "__main__":
  parser = make_uploading_parser()
  for file_name in argv[1:]:
    parser.parse(file_name)

default_feed = 'marketplace_feed_v1.xml'


# file_path = path.join(path.dirname(__file__),file_name)
