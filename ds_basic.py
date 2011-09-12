
import collections
import ctypes
import errno
import os
import stat
import string
import threading

class Token(object):
    def __init__(self, name):
        self.name = name
    def __repr__(self):
        return self.name

STAT = Token('ds_basic.STAT')
END = Token('ds_basic.END')
PARENT = Token('ds_basic.PARENT')

try:
    bytes = bytes
except NameError:
    bytes = str

try:
    unicode = unicode
except NameError:
    unicode = str

CharacterRange = collections.namedtuple('CharacterRange', ('start', 'end'))

ALL = CharacterRange(0, END)

DSTypeInfo = collections.namedtuple('DSTypeInfo', ('name', 'module', 'type'))
TopLevelInfo = collections.namedtuple('TopLevelInfo', ('key', 'typename'))

BrokenData = collections.namedtuple('BrokenData', ('description'))

def do_nothing(*args, **kwargs):
    pass

class Session(object):
    def __init__(self):
        self.open_datastores = {}
        self.lock = threading.RLock()
        self.root = self.open_datastores[()] = Root(self, '<root>', ())
        self.modules = [__import__('ds_basic'), __import__('ds_png')]
        self.refresh_modules()
        self.aliases = {}

    def refresh_modules(self):
        datastore_types = {}
        toplevels = {}
        start_magics = []

        for module in self.modules:
            for name in dir(module):
                obj = getattr(module, name)
                if isinstance(obj, type) and issubclass(obj, DataStore):
                    datastore_types[name.lower()] = DSTypeInfo(name, module, obj)
                    if '__toplevels__' in obj.__dict__:
                        for key in obj.__dict__['__toplevels__']:
                            toplevels[key.lower()] = TopLevelInfo(key, name.lower())
                    if '__start_magics__' in obj.__dict__:
                        for magic in obj.__dict__['__start_magics__']:
                            start_magics.append((magic, name))

        with self.lock:
            self.datastore_types = datastore_types
            self.toplevels = toplevels
            self.start_magics = start_magics

    def open(self, dsid, referrer):
        to_release = []
        result = None

        dsid = tuple(dsid)

        try:
            while True:
                with self.lock:
                    result = self.open_datastores.get(dsid)
                    if result:
                        result.addref(referrer)
                        break

                    i = len(dsid) - 1

                    while i >= 0:
                        if dsid[0:i] in self.open_datastores:
                            result = self.open_datastores[dsid[0:i]]
                            break
                        i -= 1
                    else:
                        raise Exception("This should never happen; no root object?")

                if i < len(dsid):
                    intermediate_dsid, klass = result.get_child_dsid(dsid[i])
                    if intermediate_dsid == dsid[0:i+1]:
                        with self.lock:
                            if intermediate_dsid not in self.open_datastores:
                                self.open_datastores[intermediate_dsid] = klass(self, '<temporary>', intermediate_dsid)
                                to_release.append(self.open_datastores[intermediate_dsid])
                    else:
                        dsid = intermediate_dsid + dsid[i+1:]

        finally:
            for ds in to_release:
                ds.release('<temporary>')

        return result

def sample_string(string, length):
    bytes = string.encode('string_escape')
    if len(bytes) > length:
        bytes = bytes[0:length] + '...'
    return "'" + bytes + "'"

class DataStore(object):
    def __init__(self, session, referrer, dsid):
        """__init__ for DataStore objects should just initialize data structures
        and return. No blocking calls or "real work" may be done in this
        function. Instead, that work should be done as needed by other functions
        and possibly cached. Subclass's __init__ methods must call the parent
        class's __init__.

        If possible, all datastores that will be needed should be reserved using
        DataStore.get_datastore.

        referrer is a string or dsid representing the object currently using
        this datastore"""
        if type(self) == DataStore:
            raise TypeError("DataStore is an abstract base class")
        self.session = session
        self._session_lock = self.session.lock
        self.referers = [referrer]
        self.references = []
        self.dsid = dsid

    def addref(self, referer):
        """addref adds a referrer for this object, preventing resources
        associated with it from being released. This function should not be
        used by datastore implementations; use DataStore.get_datastore instead."""
        with self._session_lock:
            if not self.session:
                raise ValueError("This object has been freed")
            self.referers.append(referer)

    def release(self, referer):
        """addref removes a referrer from this object. This function should not
        be used for datastores returned by DataStore.get_datastore."""
        with self._session_lock:
            if not self.session:
                raise ValueError("This object has been freed")
            self.referers.remove(referer)
            if not self.referers:
                del self.session.open_datastores[self.dsid]
                self.session = None # for breaking cycles
                self.referers = None # just in case
        if not self.session:
            self.do_free()

    def open(self, dsid, referer):
        return self.session.open(self.dsid + tuple(dsid), referer)

    def get_datastore(self, dsid):
        result = self.session.open(dsid, self.dsid)
        self.references.append(result)
        return result

    def enum_keys(self, progresscb=do_nothing):
        return iter(())

    def do_free(self):
        for reference in self.references:
            reference.release(self.dsid)

    def get_child_dsid(self, key):
        if isinstance(key, type) and issubclass(key, DataStore):
            return (self.dsid + (key,)), key
        elif isinstance(key, CharacterRange):
            if key.start == 0 and key.end is END:
                return self.dsid, type(self)
            else:
                return (self.dsid + (key,)), Slice
        elif key is PARENT:
            dsid = self.get_parent_dsid()
            if dsid == ():
                return (), Root
            else:
                grandparent_datastore = self.session.open(dsid[0:-1], '<temporary>')
                try:
                    return grandparent_datastore.get_child_dsid(dsid[-1])
                finally:
                    grandparent_datastore.release('<temporary>')
        else:
            raise ValueError("Invalid dsid: %s / %s" % (self.dsid, key))

    def get_parent_dsid(self):
        if len(self.dsid) == 1:
            return None
        return self.dsid[0:-1]

    def read_field_bytes(self, key, r=ALL, progresscb=do_nothing):
        try:
            field_range = self.locate_field(key)
        except TypeError:
            field_range = ALL
        return self.read_bytes(translate_range(field_range, r), progresscb)

    def locate_field(self, key):
        raise TypeError

    def read_bytes(self, r=ALL, progresscb=do_nothing):
        raise TypeError

    def get_description(self):
        try:
            bytes = self.read_bytes(CharacterRange(0, 21)).encode('string_escape')
            return sample_string(bytes, 20)
        except:
            return type(self).__name__

class Root(DataStore):
    def __init__(self, session, referrer, dsid):
        if dsid != ():
            raise ValueError("Root object created with non-empty dsid")
        DataStore.__init__(self, session, referrer, dsid)

    def enum_keys(self, progresscb=do_nothing):
        for toplevel in self.session.toplevels.values():
            yield toplevel.key

    def get_child_dsid(self, key):
        if isinstance(key, basestring) and key.lower() in self.session.toplevels:
            return (self.session.toplevels[key.lower()].key,), self.session.datastore_types[self.session.toplevels[key.lower()].typename].type
        elif isinstance(key, basestring):
            return ("FileSystem", key), None
        else:
            return DataStore.get_child_dsid(self, key)

    def get_parent_dsid(self):
        return ()

def translate_range(range, subrange):
    start = subrange.start + range.start

    if subrange.end is not END:
        end = subrange.end + range.start

        if range.end is not END:
            end = min(range.end, end)
    else:
        end = range.end

    return CharacterRange(start, end)

class Slice(DataStore):
    def __init__(self, session, referrer, dsid):
        DataStore.__init__(self, session, referrer, dsid)

        self.parent = self.get_datastore(dsid[0:-1])
        self.range = dsid[-1]

    def translate_range(self, range):
        return translate_range(self.range, range)

    def enum_keys(self):
        return CharacterRange(0, END if self.range.end is END else self.range.end - self.range.start)

    def read_bytes(self, r=ALL, progresscb=do_nothing):
        return self.parent.read_bytes(self.translate_range(r), progresscb)

    def get_child_dsid(self, key):
        if isinstance(key, CharacterRange):
            return (self.parent.dsid + (self.translate_range(key),)), Slice
        else:
            return DataStore.get_child_dsid(self, key)

    def get_description(self):
        result = 'stream data'
        if self.range.start != 0:
            result += ' starting at byte %s' % self.range.start
        if self.range.end is not END:
            result += ' ending at byte %s' % self.range.end
        return result

StructFieldInfo = collections.namedtuple('StructFieldInfo', ('name', 'type', 'start', 'end'))

class Data(DataStore):
    def __init__(self, session, referrer, dsid):
        DataStore.__init__(self, session, referrer, dsid)

        self.parent = self.get_datastore(dsid[0:-1])

    def enum_keys(self, progresscb=do_nothing):
        yield ALL

    def read_bytes(self, r=ALL, progresscb=do_nothing):
        return self.parent.read_field_bytes(self.dsid[-1], r, progresscb)

class UIntBE(Data):
    @classmethod
    def bytes_to_int(cls, data):
        result = 0
        for c in data:
            result = (result << 8) | ord(c)
        return result

    def get_description(self):
        return str(self.bytes_to_int(self.read_bytes()))

class CString(Data):
    def get_description(self):
        bytes = self.read_bytes()
        if bytes.endswith('\0'):
            bytes = sample_string(bytes[0:-1], 20)
        else:
            bytes = sample_string(bytes, 20) + ' (missing NULL terminator)'
        return bytes

class Boolean(UIntBE):
    def get_description(self):
        if self.read_bytes().remove('\0'):
            return 'True'
        else:
            return 'False'

class Enumeration(Data):
    def get_description(self):
        value = self.read_bytes()
        for enum_name, enum_value in self.__values__:
            if value == enum_value:
                return enum_name
        return Data.get_description(self)

    __values__ = ()

class Structure(Data):
    def _check_byte(self, ofs, checked_bytes):
        if ofs in checked_bytes or ofs is END:
            return True
        if self.read_bytes(CharacterRange(ofs, ofs + 1)):
            checked_bytes.add(ofs)
            return True
        else:
            return False

    def locate_fields(self):
        ofs = 0
        fields = {}
        field_data = {}
        field_order = []
        warnings = []
        unallocated_ranges = [(0, END)]
        checked_bytes = set()

        for field in self.__fields__:
            name, klass = field[0:2]

            start = ofs
            end = END
            optional = False
            skip = False

            for i in range(2, len(field), 2):
                setting, value = field[i:i+2]
                if setting == 'size':
                    end = start + value
                elif setting == 'size_is':
                    value = value.lower()
                    ref_field = fields[value]
                    if value not in field_data:
                        field_data[value] = self.read_bytes(CharacterRange(ref_field.start, ref_field.end))
                    data = field_data[value]
                    size = ref_field.type.bytes_to_int(data)
                    end = start + size
                elif setting == 'optional':
                    optional = True
                elif setting == 'ifequal':
                    value, expected_data = value
                    value = value.lower()
                    ref_field = fields[value]
                    if value not in field_data:
                        field_data[value] = self.read_bytes(CharacterRange(ref_field.start, ref_field.end))
                    data = field_data[value]
                    if data != expected_data:
                        skip = True
                        break
                elif setting == 'starts_with':
                    value = value.lower()
                    ref_field = fields[value]
                    start = ref_field.start
                elif setting == 'ends_with':
                    value = value.lower()
                    ref_field = fields[value]
                    end = ref_field.end
                elif setting == 'stopatnul':
                    data = self.read_bytes(CharacterRange(start, end))
                    if '\0' in data:
                        end = data.index('\0') + start + 1
                else:
                    raise TypeError("unknown structure field setting: %s" % setting)

            if skip:
                continue

            ofs = end

            if start != end:
                if not self._check_byte(start, checked_bytes):
                    if not optional:
                        warnings.append(BrokenData('Missing field %s' % name))
                    continue
                elif end is not END and not self._check_byte(end-1, checked_bytes):
                    warnings.append(BrokenData('Truncated field %s' % name))

            fields[name.lower()] = StructFieldInfo(name, klass, start, end)
            field_order.append(name)

        return fields, warnings, field_order

    def enum_keys(self, progresscb=do_nothing):
        fields, warnings, field_order = self.locate_fields()

        for field in field_order:
            yield field
        for warning in warnings:
            yield warning

    def get_child_dsid(self, key):
        if isinstance(key, basestring):
            key = key.lower()
            for field in self.__fields__:
                if key == field[0].lower():
                    return (self.dsid + (field[0],)), field[1]
            raise ValueError("Structure of type %s has no field %s\n" % (type(self).__name__, key))
        else:
            return DataStore.get_child_dsid(self, key)

    def locate_field(self, key):
        if isinstance(key, basestring):
            fields, warnings, field_order = self.locate_fields()
            if key.lower() in fields:
                field = fields[key.lower()]
                return CharacterRange(field.start, field.end)
            raise ValueError("Structure of type %s has no field %s\n" % (type(self).__name__, key))
        return DataStore.locate_field(self, key)

class FileSystemStat(DataStore):
    pass #TODO

class FileSystemObject(DataStore):
    __toplevels__ = ("FileSystem",)

    def __init__(self, session, referrer, dsid):
        DataStore.__init__(self, session, referrer, dsid)
        if len(dsid) == 1:
            path = None
        else:
            path = dsid[-1]
        if path is not None:
            path = os.path.abspath(path)        
        elif os.path.sep == '/':
            path = '/'
        self.path = path
        self.lock = threading.RLock()
        self.fd = None

    def get_fd(self, writable=False):
        while True:
            st = os.lstat(self.path)
            with self.lock:
                if self.fd is not None and writable and not self.file_writable:
                    os.close(self.fd)
                    self.fd = None
                elif self.fd is not None:
                    if st.st_ino == self.file_ino and st.st_dev == self.file_dev:
                        return self.fd, st
                    else:
                        os.close(self.fd)
                        self.fd = None

                if stat.S_ISREG(st.st_mode):
                    try:
                        if writable:
                            mode = os.O_RDWR
                        else:
                            mode = os.O_RDONLY
                        if os.path.sep == '\\':
                            mode = mode | os.O_BINARY
                        self.fd = os.open(self.path, mode)
                        if os.path.sep != '\\':
                            # This doesn't work on Windows
                            fst = os.fstat(self.fd)
                            if fst.st_ino != st.st_ino or fst.st_dev != st.st_dev:
                                # File changed since between lstat and open
                                os.close(self.fd)
                                self.fd = None
                                continue
                        self.file_writable = writable
                        self.file_ino = st.st_ino
                        self.file_dev = st.st_dev
                    except OSError, e:
                        raise
                else:
                    self.fd = None

                return self.fd, st

    def enum_keys(self, progresscb=do_nothing):
        if self.path is None:
            #windows
            drives = ctypes.windll.kernel32.GetLogicalDrives()
            for i in range(26):
                if (1 << i) & drives:
                    yield string.lowercase[i] + ':\\'
            return

        yield STAT

        st = os.lstat(self.path)
        if stat.S_ISDIR(st.st_mode):
            for entry in os.listdir(self.path):
                if isinstance(entry, unicode):
                    entry = entry.encode('utf8')
                yield entry
        elif stat.S_ISREG(st.st_mode):
            yield ALL
            # FIXME: Chain to magic number checking code
        else:
            raise NotImplementedError("not implemented for file type %x" % stat.S_IFMT(st.st_mode))

    def get_child_dsid(self, key):
        if isinstance(key, basestring):
            if isinstance(key, unicode):
                key = key.encode('utf8')
            if self.path is not None:
                key = os.path.normpath(os.path.join(self.path, key))
            if key == '/':
                return ('FileSystem',), FileSystemObject
            else:
                return ('FileSystem', key), FileSystemObject
        elif key is STAT and self.path is not None:
            return (self.dsid + (STAT,)), FileSystemStat
        else:
            return DataStore.get_child_dsid(self, key)

    def read_bytes(self, r=ALL, progresscb=do_nothing):
        if r.end == r.start:
            return ''
        with self.lock:
            fd, st = self.get_fd()
            if fd is None:
                raise IOError("Not a regular file")

            result = []
            offset = r.start
            last_res = 'x'

            os.lseek(fd, offset, os.SEEK_SET)

            if r.end == END:
                expected_end = st.st_size
            else:
                expected_end = r.end

            progresscb(offset - r.start, expected_end - offset, '')

            while last_res != '' and (r.end is END or offset < r.end):
                if r.end == END:
                    bytestoread = 4096
                else:
                    bytestoread = min(4096, r.end - offset)
                last_res = os.read(self.fd, bytestoread)
                if last_res:
                    offset += len(last_res)
                    if not progresscb(offset - r.start, expected_end - r.start, last_res):
                        result.append(last_res)

        return ''.join(result)

    def get_parent_dsid(self):
        if self.path is None or self.path == '/':
            return DataStore.get_parent_dsid(self)
        path = os.path.dirname(self.path)
        if os.path.sep == '\\' and path == self.path:
            # drive root
            return ('FileSystem',)
        elif path != self.path:
            return ('FileSystem', path)
        else:
            return ()

    def do_free(self):
        if self.fd is not None:
            os.close(self.fd)
        DataStore.do_free(self)

def key_to_unicode(key):
    if isinstance(key, bytes):
        try:
            unicode_key = key.decode('utf8')
            # FIXME: escape any unprintable characters
            return '"' + unicode_key.replace('"', '%22') + '"'
        except UnicodeDecodeError:
            raise NotImplementedError()

def key_to_bytes(key):
    if isinstance(key, bytes):
        # FIXME: escape any unprintable characters
        return '"' + key.replace('"', '%22') + '"'
    elif key is STAT:
        return 'Stat'
    elif isinstance(key, CharacterRange):
        if key.end is END:
            return '%s...' % key.start
        else:
            return '%s..%s' % (key.start, key.end)
    elif key is PARENT:
        return '..'
    elif isinstance(key, int):
        return str(key)
    elif isinstance(key, type) and issubclass(key, DataStore):
        return '?' + key.__name__
    elif isinstance(key, BrokenData):
        return 'WARNING: %s' % key.description
    else:
        raise ValueError("invalid key")

def get_dsid(datastore):
    path_elements = []
    while datastore is not None:
        datastore, key = datastore.get_parent()
        if key is None:
            return None
        path_elements.insert(0, key)
    return tuple(path_elements)

def dsid_to_bytes(dsid):
    return '/' + '/'.join(key_to_bytes(key) for key in dsid)

def bytes_to_dsid(b, base, session):
    pieces = b.split('/')
    result = list(base)
    at_start = True

    if pieces[0] == '':
        result = []
        pieces = pieces[1:]

    i = 0
    while i < len(pieces) - 1:
        if (pieces[i].count('"') % 2 == 1):
            pieces[i] = pieces[i] + '/' + pieces.pop(i+1)
        elif not pieces[i]:
            pieces.pop(i)
        else:
            i += 1
    if pieces and not pieces[i]:
        pieces.pop(i)

    for piece in pieces:
        if piece.startswith('"'):
            result.append(piece[1:-1].replace('""', '"'))
        elif piece == '.':
            pass
        elif piece == '..':
            result.append(PARENT)
        elif piece.lower() == 'stat':
            result.append(STAT)
        elif piece[0].isdigit():
            if piece.endswith('...'):
                result.append(CharacterRange(int(piece[0:-3]), END))
            elif '..' in piece:
                start, end = piece.split('..')
                result.append(CharacterRange(int(start), int(end)))
            else:
                result.append(int(piece))
        elif piece[0] == '?':
            result.append(session.datastore_types[piece[1:].lower()].type)
        elif at_start and piece == '~':
            result = ['FileSystem', os.path.expanduser('~')]
        elif at_start and piece in session.aliases:
            result = list(aliases[piece].dsid)
        else:
            result.append(piece)
        at_start = False

    return tuple(result)

