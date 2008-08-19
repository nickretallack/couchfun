from glob import iglob as glob
from sys import argv
from time import strptime

for arg in argv[1:]:
  for path in glob("%s/*/*.xml" % arg):
    feeds = path.split('/')[-2:]
    for feed in feeds:
      date = strptime(feeds[0],"%Y%m%d%H%M%S")
      print date
      
