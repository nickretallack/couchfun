"""
setup.py create -- creates the database and the views
setup.py update -- updates the views (couch_views.js)
"""

from settings import database_url
from httplib2 import Http
from couch_util import content_header, report
import simplejson as json

http = Http()
views = open("couch_views.js").read()

def create_database():
  response, content = http.request(database_url, 'PUT')
  report("Database",response,201)
  create_views()

def delete_database():
  response, content = http.request(database_url, 'DELETE')
  report("Delete Database",response,200)

def create_views():
  response,content = http.request("%s/_design/offers" % database_url,'PUT', body=views, headers=content_header)
  report("Views",response,201)

def update_views():
  response,content = http.request("%s/_design/offers" % database_url,'GET')
  report("Get Views",response,200)

  old_views = json.loads(content)
  new_views = json.loads(views)
  new_views['_id'] = old_views['_id']
  new_views['_rev'] = old_views['_rev']
  
  response,content = http.request("%s/_design/offers" % database_url,'PUT', body=json.dumps(new_views), headers=content_header)
  report("Views",response,201)

def print_usage():
  print "Usage: \tpython %s create|update|delete|recreate\n\tpython %s import <dir>" % (__file__,__file__)
  exit()

if __name__ == '__main__':
  from sys import argv
  if len(argv) < 2:
    print_usage()
    
  command = argv[1]
  if command == 'create':
    create_database()
  elif command == 'update':
    update_views()
  elif command == 'delete':
    delete_database()
  elif command == 'recreate':
    delete_database()
    create_database()
  elif command == 'import':
    if len(argv) < 3:
      print_useage()
    else:
      from feed_importer import import_dir
      import_dir(argv[2])
  else:
    print_usage()