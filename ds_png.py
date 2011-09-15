
import struct

import ds_basic

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
        length = self.read_bytes(self.locate_field("Length")[-1])
        type = self.read_bytes(self.locate_field("Type")[-1])
        if len(type) != 4:
            return "invalid PNG chunk"
        return "%s chunk of size %i" % (type, ds_basic.UIntBE.bytes_to_int(length))

class PngChunks(ds_basic.HeteroArray):
    __base_type__ = PngChunk

    def is_last_item(self, item):
        type_field = item.open(('Type',), '<temporary>')
        try:
            return type_field.read_bytes == 'IEND'
        finally:
            type_field.release('<temporary>')

class Png(ds_basic.Structure):
    __start_magics__ = ('\x89PNG\r\n\x1a\n',)

    __fields__ = (
        ('MagicNumber', ds_basic.Data, 'size', 8),
        ('Chunks', PngChunks),
        )

