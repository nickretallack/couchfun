from httplib2 import Http
import simplejson as json
content_header = {'Content-Type':'application/json'}

def report(task,response,expected):
  print "%s: %s" % (task,response.reason)
  if response.status != expected:
    print response

def prettyPrint(s):
  print json.dumps(json.loads(s), sort_keys=True, indent=2)



database_url = "http://localhost:5984/tests"
h = Http()

response, content = h.request(database_url, 'DELETE')


response, content = h.request(database_url, 'PUT')
report("Database",response,201)

docs = []
total = 9
for x in xrange(0,total):
  docs.append({'field1':x,'field2':total-x,'key':x/3})

body = '{"docs":%s}' % json.dumps(docs)

response, content = h.request("%s/_bulk_docs" % database_url, 'POST', body=body, headers=content_header)
report("Documents",response,201)


reduce_view = """
{
  "map":"function(doc){ emit([doc.key,doc.field1],{'field1':doc.field1,'field2':doc.field2})}",
  "reduce":"function(keys,values,combine){
      fields = ['field1','field2'];
      result = {};
      log(values)
      for (field in fields){
        //log(fields[field])
        result[fields[field]] = values.map(function(value){return value[fields[field]]});
      }
      log(result)
      return result;
  }"
}
"""

reduce_view2 = """
{
  "map":"function(doc){ emit([doc.key,doc.field1],{'field1':doc.field1,'field2':doc.field2})}",
  "reduce":"function(keys,values,combine){ log(keys);log(values); return values }"
}
"""


#"map":"function(doc){ emit(doc.key,doc.field1)}",

#"map":"function(doc){ emit([doc.key,doc.field1,doc.field2],{'field1':doc.field1,'field2':doc.field2})}",


response,content = h.request("%s/_temp_view" % database_url,'POST', body=reduce_view2, headers=content_header)
report("Views",response,200)
prettyPrint(content)

response,content = h.request("%s/")


