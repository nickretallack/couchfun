import web
#from web.contrib.template import render_cheetah
from settings import database_url
from httplib2 import Http
import simplejson as json
from web.cheetah import render

#import tenjin
#template = tenjin.Engine()
web.webapi.internalerror = web.debugerror   # enables debugger
#render = render_cheetah('templates')


urls = ('/','index','/(.*)','offer')

offers_per_page = 100

class index:
  def GET(self):
    page = int(web.input(page=0)['page'])
    h = Http()
    response,content = h.request('%s/_view/offers/index?count=8' % database_url)
    print response,content
    return 
    stuff = json.loads(content)    
    last_page = False
    if len(stuff['rows']) - page*offers_per_page < offers_per_page:
      last_page = True
    return render('index.html',{'offers':stuff['rows'],'page':page,'last_page':last_page})


if __name__ == "__main__":
  web.run(urls, globals(), web.reloader)

