content_header = {'Content-Type':'application/json'}

class headers:
  content = content_header

def report(task,response,expected):
  print "%s: %s" % (task,response.reason)
  if response.status != expected:
    print response
