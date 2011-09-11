
import struct

import ds_basic

class Png(ds_basic.DataStore):
    __start_magic__ = '\x89PNG\r\n\x1a\n'

    def __init__(self, session, referrer, dsid):
        ds_basic.DataStore.__init__(self, session, referrer, dsid)

        self.parent = self.get_datastore(dsid[0:-1])

    def enum_keys(self, progresscb=ds_basic.do_nothing):
        yield 'MagicNumber'
        magic = self.parent.read_bytes(ds_basic.CharacterRange(0, 8))
        if magic != self.__start_magic__:
            yield ds_basic.BrokenData('Incorrect magic number %s' % repr(magic))

        ofs = 8
        while True:
            chunk_header = self.parent.read_bytes(ds_basic.CharacterRange(ofs, ofs+8))
            if not chunk_header:
                # end of file
                return
            if len(chunk_header) < 8:
                break
            length, chunk_type = struct.unpack('>L4s', chunk_header)
            if not chunk_type.isalnum():
                # not a valid chunk
                break
            yield 'ChunkAt%i' % ofs
            if self.parent.read_bytes(ds_basic.CharacterRange(ofs + 11 + length, ofs + 11 + length + 1)) == '':
                yield ds_basic.BrokenData('Chunk at %i (length %i, type %s) is truncated' % (ofs, length, repr(chunk_type)))
            ofs += 12 + length

        if self.parent.read_bytes(ds_basic.CharacterRange(ofs, ofs+1)) != '':
            yield 'DataAt%i' % ofs

    def get_child_dsid(self, key):
        if isinstance(key, basestring) and key.lower().startswith('chunkat'):
            return (self.parent.dsid + (ds_basic.CharacterRange(int(key[7:]), ds_basic.END), PngChunk)), PngChunk
        elif isinstance(key, basestring) and key.lower() == 'magicnumber':
            return self.parent.get_child_dsid(ds_basic.CharacterRange(0, 8))
        elif isinstance(key, basestring) and key.lower().startswith('dataat'):
            return self.parent.get_child_dsid(ds_basic.CharacterRange(int(key[6:]), ds_basic.END))
        else:
            return ds_basic.DataStore.get_child_dsid(self, key)

class PngChunk(ds_basic.DataStore):
    pass

