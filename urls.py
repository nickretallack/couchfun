import web
from web.contrib.template import render_cheetah
from settings import database_url
from httplib2 import Http
import simplejson as json

#import tenjin
#template = tenjin.Engine()
web.webapi.internalerror = web.debugerror   # enables debugger
render = render_cheetah('templates')


urls = ('/','index','/(.*)','offer')

index_view = """
{
  "map":"function(doc){
    emit(null,doc)
  }"
}
"""


class index:
  def GET(self):
    #print render.index()
    h = Http()
    headers, response = h.request('%s/_temp_view?count=20' % database_url, 'POST', body=index_view,headers={"Accept": "application/json"})
    offers = json.loads(response)['rows']
    return render.index(offers=offers,name=name)
    #print offers
    #print template.render("templates/index.html",{'offers':offers})
    #print response


if __name__ == "__main__":
  web.run(urls, globals(), web.reloader)
