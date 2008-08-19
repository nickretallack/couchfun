from xml import sax

class ClickBankSaxHandler(sax.ContentHandler):
  """Parses through feeds of the sort found at clickbank.com/feeds/marketplace_feed_v1.xml.zip"""
  
  def __init__(self,import_queue=None,limit=None):
    self.limit = limit
    self.import_queue = import_queue
    if not import_queue:
      print "No Uploader"
    
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

      if self.import_queue and not self.new_site['Name'] in self.unique_sites:
        self.import_queue.add(self.new_site)

      self.unique_sites[self.new_site['Name']]= 1
      
      # in case we wanted to limit the number of imports
      self.site_tags += 1
      if self.limit and self.site_tags > self.limit:
        self.endDocument()
        exit()

      self.new_site = {}

  def endDocument(self):
    if self.import_queue:
      self.import_queue.finish()
    print "%d Sites, %d Unique" % (self.site_tags,len(self.unique_sites))

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
        if self.import_queue:
          self.import_queue.add_popularity(rank=int(string), categories=self.categories[:], site=self.new_site)
      
      # TODO: ADD DATES!
        # rename the ids too
      
  def skippedEntity(self,name):
    "This should never happen"
    print "Skipped: %s" % name
    exit()


def parse(path,import_queue=None):
  # Make an Uploading Parser
  handler = ClickBankSaxHandler(import_queue)
  parser = sax.make_parser()
  parser.setContentHandler(handler)

  # Disable a bunch of annoying features
  parser.setFeature(sax.handler.feature_namespaces,0)
  parser.setFeature(sax.handler.feature_validation,0)
  parser.setFeature(sax.handler.feature_external_ges,0)
  parser.setFeature(sax.handler.feature_external_pes,0)

  parser.parse(path)


if __name__ == "__main__":
  from sys import argv
  if len(argv) < 2:
    print "usage: python %s feedname" % __file__
  
  for feed in argv[1:]:
    parse(feed)