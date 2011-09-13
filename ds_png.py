
import struct

import ds_basic

class Png(ds_basic.Data):
    __start_magics__ = ('\x89PNG\r\n\x1a\n',)

    def enum_keys(self, progresscb=ds_basic.do_nothing):
        yield 'MagicNumber'
        magic = self.read_bytes(ds_basic.CharacterRange(0, 8))
        if magic not in self.__start_magics__:
            yield ds_basic.BrokenData('Incorrect magic number %s' % repr(magic))

        ofs = 8
        while True:
            chunk_header = self.read_bytes(ds_basic.CharacterRange(ofs, ofs+8))
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
            if self.read_bytes(ds_basic.CharacterRange(ofs + 11 + length, ofs + 11 + length + 1)) == '':
                yield ds_basic.BrokenData('Chunk at %i (length %i, type %s) is truncated' % (ofs, length, repr(chunk_type)))
            ofs += 12 + length

        if self.read_bytes(ds_basic.CharacterRange(ofs, ofs+1)) != '':
            yield 'DataAt%i' % ofs

    def get_child_dsid(self, key):
        if isinstance(key, basestring):
            if key.lower().startswith('chunkat'):
                return (self.dsid + ('ChunkAt' + key[7:],)), PngChunk
            elif key.lower().startswith('magicnumber'):
                return (self.dsid + ('MagicNumber',)), ds_basic.Data
            elif key.lower().startswith('dataat'):
                return (self.dsid + ('DataAt' + key[6:],)), ds_basic.Data
            else:
                raise ValueError("invalid field %s" % key)
        else:
            return ds_basic.DataStore.get_child_dsid(self, key)

    def locate_field(self, key):
        if isinstance(key, basestring):
            if key.lower().startswith('chunkat'):
                return ds_basic.CharacterRange(int(key[7:]), ds_basic.END)
            elif key.lower().startswith('magicnumber'):
                return ds_basic.CharacterRange(0, 8)
            elif key.lower().startswith('dataat'):
                return ds_basic.CharacterRange(int(key[6:]), ds_basic.END)
            else:
                raise ValueError("invalid field %s" % key)
        else:
            return ds_basic.DataStore.locate_field(self, key)

class PngChunkCrc(ds_basic.UIntBE):
    pass

class PngColorType(ds_basic.Enumeration):
    __values__ = (
        ('Grayscale', '\x00'),
        ('RGB', '\x02'),
        ('Palette', '\x03'),
        ('Grayscale+Alpha', '\x04'),
        ('RGBA', '\x06'),
        )

class PngCompressionMethod(ds_basic.Enumeration):
    __values__ = (
        ('Deflate', '\x00'),
        )

class PngFilterMethod(ds_basic.Enumeration):
    __values__ = (
        ('Adaptive', '\x00'),
        )

class PngInterlaceMethod(ds_basic.Enumeration):
    __values__ = (
        ('None', '\x00'),
        ('Adam7', '\x01'),
        )

class PngHeader(ds_basic.Structure):
    __fields__ = (
        ('Width', ds_basic.UIntBE, 'size', 4),
        ('Height', ds_basic.UIntBE, 'size', 4),
        ('BitDepth', ds_basic.UIntBE, 'size', 1),
        ('ColorType', PngColorType, 'size', 1),
        ('CompressionMethod', PngCompressionMethod, 'size', 1),
        ('FilterMethod', PngFilterMethod, 'size', 1),
        ('InterlaceMethod', PngInterlaceMethod, 'size', 1),
        )

class PngChromaticities(ds_basic.Structure):
    __fields__ = (
        ('WhitePointX', ds_basic.UIntBE, 'size', 4),
        ('WhitePointY', ds_basic.UIntBE, 'size', 4),
        ('RedX', ds_basic.UIntBE, 'size', 4),
        ('RedY', ds_basic.UIntBE, 'size', 4),
        ('GreenX', ds_basic.UIntBE, 'size', 4),
        ('GreenY', ds_basic.UIntBE, 'size', 4),
        ('BlueX', ds_basic.UIntBE, 'size', 4),
        ('BlueY', ds_basic.UIntBE, 'size', 4),
        )

class PngRenderingIntent(ds_basic.Enumeration):
    __values__ = (
        ('Perceptual', '\x00'),
        ('RelativeColorimetric', '\x01'),
        ('Saturation', '\x02'),
        ('AbsoluteColorimetric', '\x03'),
        )

class PngIccProfile(ds_basic.Structure):
    __fields__ = (
        ('ProfileName', ds_basic.CString),
        ('CompressionMethod', PngCompressionMethod, 'size', 1),
        ('CompressedProfile', ds_basic.Data),
        )
    # FIXME: Test this and make it possible to uncompress the data

class PngText(ds_basic.Structure):
    __fields__ = (
        ('Keyword', ds_basic.CString),
        ('Text', ds_basic.Data),
        )

class PngTextZ(ds_basic.Structure):
    __fields__ = (
        ('Keyword', ds_basic.CString),
        ('CompressionMethod', PngCompressionMethod, 'size', 1),
        ('CompressedText', ds_basic.Data),
        )
    # FIXME: Test this and make it possible to uncompress the data

class PngTextI(ds_basic.Structure):
    __fields__ = (
        ('Keyword', ds_basic.CString),
        ('CompressionFlag', ds_basic.Boolean, 'size', 1),
        ('CompressionMethod', PngCompressionMethod, 'size', 1),
        ('LanguageTag', ds_basic.CString),
        ('TranslatedKeyword', ds_basic.CString),
        ('RawText', ds_basic.Data),
        )
    # FIXME: Test this and make it possible to uncompress the data

class PngPhysUnit(ds_basic.Enumeration):
    __values__ = (
        ('Unknown', '\x00'),
        ('Meter', '\x01'),
        )

class PngPhys(ds_basic.Structure):
    __fields__ = (
        ('XPixelsPerUnit', ds_basic.UIntBE, 'size', 4),
        ('YPixelsPerUnit', ds_basic.UIntBE, 'size', 4),
        ('Unit', PngPhysUnit, 'size', 1),
        )

# FIXME: sPLt not parsed because it's too complicated for Structure

class PngTime(ds_basic.Structure):
    __fields__ = (
        ('Year', ds_basic.UIntBE, 'size', 2),
        ('Month', ds_basic.UIntBE, 'size', 1),
        ('Day', ds_basic.UIntBE, 'size', 1),
        ('Hour', ds_basic.UIntBE, 'size', 1),
        ('Minute', ds_basic.UIntBE, 'size', 1),
        ('Second', ds_basic.UIntBE, 'size', 1),
        )

class PngChunk(ds_basic.Structure):
    __fields__ = (
        ('Length', ds_basic.UIntBE, 'size', 4),
        ('Type', ds_basic.Data, 'size', 4),
        ('RawData', ds_basic.Data, 'size_is', 'Length'),
        ('CRC', PngChunkCrc, 'size', 4),
        ('Header', PngHeader, 'ifequal', ('Type', 'IHDR'), 'starts_with', 'RawData', 'ends_with', 'RawData'),
        ('Gamma', ds_basic.UIntBE, 'ifequal', ('Type', 'gAMA'), 'starts_with', 'RawData', 'size', 4),
        ('Chromaticities', PngChromaticities, 'ifequal', ('Type', 'cHRM'), 'starts_with', 'RawData', 'ends_with', 'RawData'),
        ('IccProfile', PngIccProfile, 'ifequal', ('Type', 'iCCP'), 'starts_with', 'RawData', 'ends_with', 'RawData'),
        ('Text', PngText, 'ifequal', ('Type', 'tEXt'), 'starts_with', 'RawData', 'ends_with', 'RawData'),
        ('TextZ', PngTextZ, 'ifequal', ('Type', 'xTXt'), 'starts_with', 'RawData', 'ends_with', 'RawData'),
        ('TextI', PngTextI, 'ifequal', ('Type', 'iTXt'), 'starts_with', 'RawData', 'ends_with', 'RawData'),
        ('PhysicalDimensions', PngPhys, 'ifequal', ('Type', 'pHYs'), 'starts_with', 'RawData', 'ends_with', 'RawData'),
        ('MTime', PngTime, 'ifequal', ('Type', 'tIME'), 'starts_with', 'RawData', 'ends_with', 'RawData'),
        )

    def get_description(self):
        length = self.read_field_bytes("Length")
        type = self.read_field_bytes("Type")
        if len(type) != 4:
            return "invalid PNG chunk"
        return "%s chunk of size %i" % (type, ds_basic.UIntBE.bytes_to_int(length))

