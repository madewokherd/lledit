
import struct

import ds_basic

class Png(ds_basic.Data):
    __start_magics__ = ('\x89PNG\r\n\x1a\n',)

    def enum_keys(self, progresscb=ds_basic.do_nothing):
        yield 'MagicNumber'
        magic = self.parent.read_bytes(ds_basic.CharacterRange(0, 8))
        if magic not in self.__start_magics__:
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

class PngChunkCrc(ds_basic.UIntBE):
    pass

class PngChunk(ds_basic.Structure):
    def __init__(self, session, referrer, dsid):
        ds_basic.Structure.__init__(self, session, referrer, dsid)

    subfields = True

    __fields__ = (
        ('Length', ds_basic.UIntBE, 'size', 4),
        ('Type', ds_basic.Data, 'size', 4),
        ('RawData', ds_basic.Data, 'size_is', 'Length'),
        ('CRC', PngChunkCrc, 'size', 4),
        ('Next', ds_basic.Data),
        )

    def get_description(self):
        length = self.read_field_bytes("Length")
        type = self.read_field_bytes("Type")
        if len(type) != 4:
            return "invalid PNG chunk"
        return "%s chunk of size %i" % (type, ds_basic.UIntBE.bytes_to_int(length))

