'''
Parser for multipart/form-data
==============================

This module provides a parser for the multipart/form-data format. It can read
from a file, a socket or a WSGI environment.
'''
import re
import sys
from tempfile import TemporaryFile
from wsgiref.headers import Headers
from base64 import b64encode
from io import BytesIO

from .httpurl import parse_qs, ENCODE_BODY_METHODS, mapping_iterator
from .structures import MultiValueDict


def copy_file(stream, target, maxread=-1, buffer_size=2*16):
    ''' Read from :stream and write to :target until :maxread or EOF. '''
    size, read = 0, stream.read
    while 1:
        to_read = buffer_size if maxread < 0 else min(buffer_size,
                                                      maxread-size)
        part = read(to_read)
        if not part:
            return size
        target.write(part)
        size += len(part)

# ############################################################################
# ############################## Header Parser ###############################
# ############################################################################

_special = re.escape('()<>@,;:\\"/[]?={} \t')
_re_special = re.compile('[%s]' % _special)
_qstr = '"(?:\\\\.|[^"])*"'  # Quoted string
_value = '(?:[^%s]+|%s)' % (_special, _qstr)  # Save or quoted string
_option = '(?:;|^)\s*([^%s]+)\s*=\s*(%s)' % (_special, _value)
_re_option = re.compile(_option)  # key=value part of an Content-Type header


def header_quote(val):
    if not _re_special.search(val):
        return val
    return '"' + val.replace('\\', '\\\\').replace('"', '\\"') + '"'


def header_unquote(val, filename=False):
    if val[0] == val[-1] == '"':
        val = val[1:-1]
        if val[1:3] == ':\\' or val[:2] == '\\\\':
            val = val.split('\\')[-1]  # fix ie6 bug: full path --> filename
        return val.replace('\\\\', '\\').replace('\\"', '"')
    return val


def parse_options_header(header, options=None):
    if ';' not in header:
        return header.lower().strip(), {}
    ctype, tail = header.split(';', 1)
    options = options or {}
    for match in _re_option.finditer(tail):
        key = match.group(1).lower()
        value = header_unquote(match.group(2), key == 'filename')
        options[key] = value
    return ctype, options

# ############################################################################
# ################################ Multipart #################################
# ############################################################################


class MultipartError(ValueError):
    pass


class MultipartParser(object):

    def __init__(self, stream, boundary, content_length=-1,
                 disk_limit=2**30, mem_limit=2**20, memfile_limit=2**18,
                 buffer_size=2**16, charset='latin1'):
        ''' Parse a multipart/form-data byte stream. This object is an
        iterator over the parts of the message.

        :param stream: A file-like stream. Must implement ``.read(size)``.
        :param boundary: The multipart boundary as a byte string.
        :param content_length: The maximum number of bytes to read.
        '''
        self.stream, self.boundary = stream, boundary
        self.content_length = content_length
        self.disk_limit = disk_limit
        self.memfile_limit = memfile_limit
        self.mem_limit = min(mem_limit, self.disk_limit)
        self.buffer_size = min(buffer_size, self.mem_limit)
        self.charset = charset
        if self.buffer_size - 6 < len(boundary):  # "--boundary--\r\n"
            raise MultipartError('Boundary does not fit into buffer_size.')
        self._done = []
        self._part_iter = None
        self.separator = '--{0}'.format(self.boundary).encode()
        self.terminator = '--{0}--'.format(self.boundary).encode()

    def __iter__(self):
        ''' Iterate over the parts of the multipart message. '''
        if not self._part_iter:
            self._part_iter = self._iterparse()
        for part in self._done:
            yield part
        for part in self._part_iter:
            self._done.append(part)
            yield part

    def parts(self):
        ''' Returns a list with all parts of the multipart message. '''
        return list(iter(self))

    def get(self, name, default=None):
        ''' Return the first part with that name or a default value (None). '''
        for part in self:
            if name == part.name:
                return part
        return default

    def get_all(self, name):
        ''' Return a list of parts with that name. '''
        return [p for p in self if p.name == name]

    def _lineiter(self):
        ''' Iterate over a binary file-like object line by line. Each line is
            returned as a (line, line_ending) tuple. If the line does not fit
            into self.buffer_size, line_ending is empty and the rest of the
            lineis returned with the next iteration.
        '''
        read = self.stream.read
        maxread, maxbuf = self.content_length, self.buffer_size
        _bcrnl = b'\r\n'
        _bcr = _bcrnl[:1]
        _bnl = _bcrnl[1:]
        _bempty = _bcrnl[:0]  # b'rn'[:0] -> b''
        buffer = _bempty  # buffer for the last (partial) line
        while 1:
            data = read(maxbuf if maxread < 0 else min(maxbuf, maxread))
            maxread -= len(data)
            lines = (buffer+data).splitlines(True)
            len_first_line = len(lines[0])
            # be sure that the first line does not become too big
            if len_first_line > self.buffer_size:
                # at the same time don't split a '\r\n' accidentally
                if (len_first_line == self.buffer_size+1 and
                        lines[0].endswith(_bcrnl)):
                    splitpos = self.buffer_size - 1
                else:
                    splitpos = self.buffer_size
                lines[:1] = [lines[0][:splitpos],
                             lines[0][splitpos:]]
            if data:
                buffer = lines[-1]
                lines = lines[:-1]
            for line in lines:
                if line.endswith(_bcrnl):
                    yield line[:-2], _bcrnl
                elif line.endswith(_bnl):
                    yield line[:-1], _bnl
                elif line.endswith(_bcr):
                    yield line[:-1], _bcr
                else:
                    yield line, _bempty
            if not data:
                break

    def _iterparse(self):
        lines, line = self._lineiter(), ''
        separator = self.separator
        terminator = self.terminator
        # Consume first boundary. Ignore leading blank lines
        for line, nl in lines:
            if line:
                break
        if line != separator:
            raise MultipartError("Stream does not start with boundary")
        # For each part in stream...
        mem_used, disk_used = 0, 0  # Track used resources to prevent DoS
        is_tail = False  # True if the last line was incomplete (cutted)
        opts = {'buffer_size': self.buffer_size,
                'memfile_limit': self.memfile_limit,
                'charset': self.charset}
        part = MultipartPart(**opts)
        for line, nl in lines:
            if line == terminator and not is_tail:
                part.file.seek(0)
                yield part
                break
            elif line == separator and not is_tail:
                if part.is_buffered():
                    mem_used += part.size
                else:
                    disk_used += part.size
                part.file.seek(0)
                yield part
                part = MultipartPart(**opts)
            else:
                is_tail = not nl  # The next line continues this one
                part.feed(line, nl)
                if part.is_buffered():
                    if part.size + mem_used > self.mem_limit:
                        raise MultipartError("Memory limit reached.")
                elif part.size + disk_used > self.disk_limit:
                    raise MultipartError("Disk limit reached.")
        if line != terminator:
            raise MultipartError("Unexpected end of multipart stream.")


class MultipartPart(object):
    default_charset = 'latin1'

    def __init__(self, buffer_size=2**16, memfile_limit=2**18, charset=None):
        self.headerlist = []
        self.headers = None
        self.file = False
        self.size = 0
        self._buf = b''
        self.disposition, self.name, self.filename = None, None, None
        self.content_type = None
        self.charset = charset or self.default_charset
        self.memfile_limit = memfile_limit
        self.buffer_size = buffer_size

    def feed(self, line, nl=''):
        if self.file:
            return self.write_body(line, nl)
        return self.write_header(line, nl)

    def write_header(self, line, nl):
        line = line.decode(self.charset)
        if not nl:
            raise MultipartError('Unexpected end of line in header.')
        if not line.strip():  # blank line -> end of header segment
            self.finish_header()
        elif line[0] in ' \t' and self.headerlist:
            name, value = self.headerlist.pop()
            self.headerlist.append((name, value+line.strip()))
        else:
            if ':' not in line:
                raise MultipartError("Syntax error in header: No colon.")
            name, value = line.split(':', 1)
            self.headerlist.append((name.strip(), value.strip()))

    def write_body(self, line, nl):
        if not line and not nl:  # This does not even flush the buffer
            return
        self.size += len(line) + len(self._buf)
        self.file.write(self._buf + line)
        self._buf = nl
        if self.content_length > 0 and self.size > self.content_length:
            raise MultipartError('Size of body exceeds Content-Length header.')
        if self.size > self.memfile_limit and isinstance(self.file, BytesIO):
            # TODO: What about non-file uploads that exceed the memfile_limit?
            self.file, old = TemporaryFile(mode='w+b'), self.file
            old.seek(0)
            copy_file(old, self.file, self.size, self.buffer_size)

    def finish_header(self):
        self.file = BytesIO()
        self.headers = Headers(self.headerlist)
        cdis = self.headers.get('Content-Disposition', '')
        ctype = self.headers.get('Content-Type', '')
        clen = self.headers.get('Content-Length', '-1')
        if not cdis:
            raise MultipartError('Content-Disposition header is missing.')
        self.disposition, self.options = parse_options_header(cdis)
        self.name = self.options.get('name')
        self.filename = self.options.get('filename')
        self.content_type, options = parse_options_header(ctype)
        self.charset = options.get('charset') or self.charset
        self.content_length = int(self.headers.get('Content-Length', '-1'))

    def is_buffered(self):
        ''' Return true if the data is fully buffered in memory.'''
        return isinstance(self.file, BytesIO)

    def bytes(self):
        pos = self.file.tell()
        self.file.seek(0)
        val = self.file.read()
        self.file.seek(pos)
        return val

    def base64(self, charset=None):
        '''Data encoded as base 64'''
        return b64encode(self.bytes()).decode(charset or self.charset)

    def string(self, charset=None):
        '''Data decoded with the specified charset'''
        return self.bytes().decode(charset or self.charset)

    def save_as(self, path):
        fp = open(path, 'wb')
        pos = self.file.tell()
        try:
            self.file.seek(0)
            size = copy_file(self.file, fp)
        finally:
            self.file.seek(pos)
        return size


def parse_form_data(environ, charset='utf-8', strict=False, **kw):
    '''Parse form data from an environ dict and return a (forms, files) tuple.
Both tuple values are dictionaries with the form-field name as a key
(unicode) and lists as values (multiple values per key are possible).
The forms-dictionary contains form-field values as unicode strings.
The files-dictionary contains :class:`MultipartPart` instances, either
because the form-field was a file-upload or the value is to big to fit
into memory limits.

:parameter environ: A WSGI environment dict.
:parameter charset: The charset to use if unsure. (default: utf8)
:parameter strict: If True, raise :exc:`MultipartError` on any parsing
    errors. These are silently ignored by default.'''
    forms, files = MultiValueDict(), MultiValueDict()
    try:
        if (environ.get('REQUEST_METHOD', 'GET').upper()
                not in ENCODE_BODY_METHODS):
            raise MultipartError("Request method not valid.")
        content_length = int(environ.get('CONTENT_LENGTH', '-1'))
        content_type = environ.get('CONTENT_TYPE', '')
        if not content_type:
            raise MultipartError("Missing Content-Type header.")
        content_type, options = parse_options_header(content_type)
        stream = environ.get('wsgi.input') or BytesIO()
        kw['charset'] = charset = options.get('charset', charset)
        if content_type == 'multipart/form-data':
            boundary = options.get('boundary', '')
            if not boundary:
                raise MultipartError("No boundary for multipart/form-data.")
            for part in MultipartParser(stream, boundary,
                                        content_length, **kw):
                if part.filename or not part.is_buffered():
                    files[part.name] = part
                else:
                    forms[part.name] = part.string()
        elif content_type in ('application/x-www-form-urlencoded',
                              'application/x-url-encoded'):
            mem_limit = kw.get('mem_limit', 2**20)
            if content_length > mem_limit:
                raise MultipartError("Request to big. Increase MAXMEM.")
            data = stream.read(mem_limit).decode(charset)
            if stream.read(1):  # These is more that does not fit mem_limit
                raise MultipartError("Request to big. Increase MAXMEM.")
            data = parse_qs(data, keep_blank_values=True)
            for key, values in mapping_iterator(data):
                for value in values:
                    forms[key] = value
        else:
            raise MultipartError("Unsupported content type.")
    except MultipartError:
        if strict:
            raise
    return forms, files
