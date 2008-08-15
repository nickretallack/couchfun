from settings import database_url
from httplib2 import Http

content_header = {'Content-Type':'application/json'}

def report(task,response,expected):
  print "%s: %s" % (task,response.reason)
  if response.status != expected:
    print response



h = Http()
response, content = h.request(database_url, 'PUT')
report("Database",response,201)

offer_views = """
{
  "language":"javascript",
  "views":
  {
    "index":{"map":"function(doc){ emit(doc.Name,
    {
    // pies
    'Commission':doc.Commission,
    'Chargeback':doc.Commission - doc.PercentPerSale,

    // Sparklines
    'Gravity':doc.Gravity, 
    'Referred':doc.Referred, 
    'Rebill':doc.TotalRebillAmt,
    
    // TODO: fetch categories/popularity
    // TODO: fetch keyword classification counts

    })}"}
  }
}
"""

response,content = h.request("%s/_design/offers" % database_url,'PUT', body=offer_views, headers=content_header)
report("Views",response,201)
