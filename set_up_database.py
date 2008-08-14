from settings import database_url
from httplib2 import Http

h = Http()
response, headers = h.request(database_url, 'PUT')