import zlib


class GzipDecompress:

    def __call__(self, data):
        deco = zlib.decompressobj(16 + zlib.MAX_WBITS)
        return deco.decompress(data)


class DeflateDecompress:

    def __call__(self, data):
        try:
            return zlib.decompressobj().decompress(data)
        except zlib.error:
            return zlib.decompressobj(-zlib.MAX_WBITS).decompress(data)
