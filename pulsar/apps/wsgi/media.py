import os
import re
import stat
import mimetypes
from email.utils import parsedate_tz, mktime_tz

from pulsar.utils.httpurl import http_date, CacheControl, remove_double_slash
from pulsar import Http404, PermissionDenied

from .wsgi import Router
from .content import Html

__all__ = ['MediaRouter']
    

class MediaRouter(Router):
    default_content_type = 'application/octet-stream'
    cache_control = CacheControl(maxage=86400)

    def __init__(self, rule, path, show_indexes=True):
        super(MediaRouter, self).__init__('%s/<path:path>' % rule)
        self._show_indexes = show_indexes
        self._file_path = path
        
    def get(self, request):
        paths = request.urlargs['path'].split('/')
        if len(paths) == 1 and paths[0] == '':
            paths.pop(0)
        fullpath = os.path.join(self._file_path, *paths)
        if os.path.isdir(fullpath):
            if self._show_indexes:
                return self.directory_index(request, fullpath)
            else:
                raise PermissionDenied()
        elif os.path.exists(fullpath):
            return self.serve_file(request, fullpath)
        else:
            raise Http404()

    def directory_index(self, request, fullpath):
        names = [Html('a', '../', href='../', cn='folder')]
        files = []
        for f in sorted(os.listdir(fullpath)):
            if not f.startswith('.'):
                if os.path.isdir(os.path.join(fullpath, f)):
                    names.append(Html('a', f, href=f+'/', cn='folder'))
                else:
                    files.append(Html('a', f, href=f))
        names.extend(files)
        return self.static_index(request, names)
    
    def html_title(self, request):
        return 'Index of %s' % request.path
    
    def static_index(self, request, links):
        title = Html('h2', self.html_title(request))
        list = Html('ul', *[Html('li', a) for a in links])
        body = Html('div', title, list)
        doc = request.html_document(title=title, body=body)
        return doc.http_response(request)

    def serve_file(self, request, fullpath):
        # Respect the If-Modified-Since header.
        statobj = os.stat(fullpath)
        mimetype, encoding = mimetypes.guess_type(fullpath)
        mimetype = mimetype or self.default_content_type
        response = request.response
        response.content_type = mimetype
        response.encoding = encoding
        if not self.was_modified_since(request.environ.get(
                                            'HTTP_IF_MODIFIED_SINCE'),
                                       statobj[stat.ST_MTIME],
                                       statobj[stat.ST_SIZE]):
            response.status_code = 304
        else:
            response.content = open(fullpath, 'rb').read()
            response.headers["Last-Modified"] = http_date(statobj[stat.ST_MTIME])
        return response.start()

    def was_modified_since(self, header=None, mtime=0, size=0):
        """
        Was something modified since the user last downloaded it?

        header
          This is the value of the If-Modified-Since header.  If this is None,
          I'll just return True.

        mtime
          This is the modification time of the item we're talking about.

        size
          This is the size of the item we're talking about.
        """
        try:
            if header is None:
                raise ValueError
            matches = re.match(r"^([^;]+)(; length=([0-9]+))?$", header,
                               re.IGNORECASE)
            header_mtime = mktime_tz(parsedate_tz(matches.group(1)))
            header_len = matches.group(3)
            if header_len and int(header_len) != size:
                raise ValueError()
            if mtime > header_mtime:
                raise ValueError()
        except (AttributeError, ValueError, OverflowError):
            return True
        return False


class FavIconView:

    def get(self, request):
        config = request.config
        mapping = self.media_mapping
        name = config.get('FAVICON_MODULE') or config.get('SITE_MODULE')
        if name not in mapping:
            name = 'lux'
        hd = mapping[name]
        fullpath = os.path.join(hd.absolute_path, 'favicon.ico')
        return self.serve_file(request, fullpath)
