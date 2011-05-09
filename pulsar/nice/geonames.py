import json
from pulsar.http import HttpClient, urlencode
 
# http://www.geonames.org/export/geonames-search.html
 
def geocode(q, http = None):
    http = http or HttpClient()
    response = http.request('http://ws.geonames.org/searchJSON?' + urlencode({
        'q': q,
        'maxRows': 1,
        'lang': 'en',
        'style': 'full'
    }))
    if response.status == 200:
        data = json.loads(response.content.decode())
        if not data['geonames']:
            return None
        return data['geonames'][0]
 

if __name__ == '__main__':
    r = geocode('london')
    print(r)