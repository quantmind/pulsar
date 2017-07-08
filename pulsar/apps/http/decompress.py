import zlib


class GzipDecompress:

    def __init__(self):
        self.deco = zlib.decompressobj(16 + zlib.MAX_WBITS)

    def __call__(self, data):
        return self.deco.decompress(data)


class DeflateDecompress:

    def __init__(self):
        self.deco1 = zlib.decompressobj()
        self.deco2 = zlib.decompressobj(-zlib.MAX_WBITS)

    def __call__(self, data):
        try:
            return self.deco1.decompress(data)
        except zlib.error:
            return self.deco2.decompress(data)
