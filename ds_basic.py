
import collections
import ctypes
import errno
import os
import stat
import string
import tempfile
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
        self.modified_datastores = set()

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

    def get_open_datastores(self):
        result = []
        with self.lock:
            for key, value in self.open_datastores.iteritems():
                if key == ():
                    continue
                result.append((key, value.referers[:]))
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
        self.referers = [referrer]
        self.references = []
        self.dsid = dsid

    def addref(self, referer):
        """addref adds a referrer for this object, preventing resources
        associated with it from being released. This function should not be
        used by datastore implementations; use DataStore.get_datastore instead."""
        with self.session.lock:
            if not self.session:
                raise ValueError("This object has been freed")
            self.referers.append(referer)

    def release(self, referer):
        """addref removes a referrer from this object. This function should not
        be used for datastores returned by DataStore.get_datastore."""
        with self.session.lock:
            if not self.session:
                raise ValueError("This object has been freed")
            self.referers.remove(referer)
            if not self.referers:
                for reference in self.references[:]:
                    self.release_datastore(reference)
                del self.session.open_datastores[self.dsid]
                self.referers = None # just in case
        if not self.session:
            self.do_free()

    def open(self, dsid, referer):
        return self.session.open(self.dsid + tuple(dsid), referer)

    def get_datastore(self, dsid):
        result = self.session.open(dsid, self.dsid)
        self.references.append(result)
        return result

    def release_datastore(self, datastore):
        self.references.remove(datastore)
        datastore.release(self.dsid)

    def enum_keys(self, progresscb=do_nothing):
        return iter(())

    def do_free(self):
        for reference in self.references:
            reference.release(self.dsid)

    def get_child_dsid(self, key):
        if isinstance(key, type) and issubclass(key, DataStore):
            return (self.dsid + (key,)), key
        elif isinstance(key, CharacterRange):
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

    def locate_field(self, key):
        if isinstance(key, type) and issubclass(key, DataStore):
            try:
                return self.dsid + (CharacterRange(0, self.get_size()),)
            except TypeError:
                pass
        raise TypeError

    def read_bytes(self, r=ALL, progresscb=do_nothing):
        raise TypeError

    def get_description(self):
        try:
            bytes = self.read_bytes(CharacterRange(0, 21)).encode('string_escape')
            return sample_string(bytes, 20)
        except:
            return type(self).__name__

    def get_size(self):
        raise TypeError

    def on_change(self, datastore, key, requestor):
        pass

    def notify_change(self, key, requestor):
        with self.session.lock:
            for referer in self.referers:
                try:
                    f = referer.on_change
                except AttributeError:
                    continue
                else:
                    f(self, key, requestor)

    def copyto(self, dst_datastore, requestor, options, progresscb=do_nothing):
        raise TypeError

    def write(self, src_datastore, requestor, options, progresscb=do_nothing):
        return src_datastore.copyto(self, requestor, options, progresscb)

    def write_bytes(self, src_datastore, requestor, r=ALL, progresscb=do_nothing):
        raise TypeError

    def commit(self, progresscb=do_nothing):
        raise TypeError

    def set_modified(self):
        with self.session.lock:
            if self not in self.session.modified_datastores:
                self.session.modified_datastores.add(self)
                self.addref('<modified>')

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

def range_union(r, sibling_range):
    start = max(r.start, sibling_range.start)

    if r.end is END or (sibling_range.end is not END and r.end > sibling_range.end):
        end = sibling_range.end
    else:
        end = r.end

    if end is not END and end <= start:
        return None

    return CharacterRange(start, end)

def range_offset(r, n):
    return CharacterRange(r.start + n, END if r.end is END else r.end + n)

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

    def get_size(self):
        if self.range.end is END:
            return END
        else:
            return self.range.end - self.range.start

    def on_change(self, datastore, key, requestor):
        if datastore == self.parent and isinstance(key, CharacterRange):
            union = range_union(self.range, key)
            if union is not None:
                self.notify_change(range_offset(union, -self.range.start), requestor)

    def write(self, src_datastore, requestor, options, progresscb=do_nothing):
        return self.parent.write_bytes(src_datastore, requestor, self.range, progresscb)

DataFieldInfo = collections.namedtuple('DataFieldInfo', ('name', 'path', 'type', 'start', 'end'))

class Data(DataStore):
    __fields__ = ()

    def __init__(self, session, referrer, dsid):
        DataStore.__init__(self, session, referrer, dsid)

        self.parent = self.get_datastore(dsid[0:-1])
        self.rawdata = (None, 0)

    def get_rawdata(self):
        while True:
            rawdata, times_refreshed = self.rawdata
            if rawdata is None:
                field = self.parent.locate_field(self.dsid[-1])
                with self.session.lock:
                    rawdata, new_times_refreshed = self.rawdata
                    if new_times_refreshed == times_refreshed:
                        self.rawdata = (self.get_datastore(field), times_refreshed+1)
            else:
                return rawdata

    def read_bytes(self, r=ALL, progresscb=do_nothing):
        return self.get_rawdata().read_bytes(r, progresscb)

    def get_child_dsid(self, key):
        if isinstance(key, basestring):
            key = key.lower()
            for field in self.__fields__:
                if key == field[0].lower():
                    return (self.dsid + (field[0],)), field[1]
            raise ValueError("Structure of type %s has no field %s\n" % (type(self).__name__, key))
        else:
            return DataStore.get_child_dsid(self, key)

    def locate_fields(self):
        return {}, [], []

    def enum_keys(self, progresscb=do_nothing):
        fields, warnings, field_order = self.locate_fields()

        size = self.get_size()
        if size:
            unused_ranges = [CharacterRange(0, size)]
        else:
            unused_ranges = ()

        for field in field_order:
            yield field
            field = fields[field.lower()]
            if field.type:
                i = 0
                while i < len(unused_ranges):
                    if field.end is not END and field.end <= unused_ranges[i].start:
                        i = i + 1
                    elif unused_ranges[i].end is not END and field.start >= unused_ranges[i].end:
                        break
                    elif field.start <= unused_ranges[i].start and (field.end is END or 
                        (unused_ranges[i].end is not END and field.end >= unused_ranges[i].end)):
                        unused_ranges.pop(i)
                    elif field.start <= unused_ranges[i].start:
                        unused_ranges[i] = CharacterRange(field.end, unused_ranges[i].end)
                        break
                    elif field.end is END or (unused_ranges[i].end is not END and field.end >= unused_ranges[i].end):
                        unused_ranges[i] = CharacterRange(unused_ranges[i].start, field.start)
                        i = i + 1
                    else:
                        unused_ranges.insert(i+1, CharacterRange(field.end, unused_ranges[i].start))
                        unused_ranges[i] = CharacterRange(unused_ranges[i].start, field.start)
                        break

        for r in unused_ranges:
            yield r

    def locate_field(self, key):
        if isinstance(key, basestring):
            fields, warnings, field_order = self.locate_fields()
            if key.lower() in fields:
                field = fields[key.lower()]
                return self.dsid + (CharacterRange(field.start, field.end),)
            raise ValueError("Structure of type %s has no field %s\n" % (type(self).__name__, key))
        return DataStore.locate_field(self, key)

    def locate_end(self):
        fields, warnings, field_order = self.locate_fields()

        end = -1

        for field in fields:
            field = fields[field.lower()]

            if field.type:
                if field.end == END:
                    return END
                elif field.end > end:
                    end = field.end

        if end == -1:
            return END

        return end

    def get_size(self):
        return self.get_rawdata().get_size()

    def on_change(self, datastore, key, requestor):
        changed_range = None
        with self.session.lock:
            if datastore is self.parent and key == self.dsid[-1]:
                self.rawdata = (None, self.rawdata[1]+1)
                changed_range = ALL
            elif datastore is self.rawdata and isinstance(key, CharacterRange):
                changed_range = key
        if changed_range is not None:
            self.notify_change(changed_range, requestor)

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
    def __init__(self, session, referrer, dsid):
        DataStore.__init__(self, session, referrer, dsid)

    def get_description(self):
        bytes = self.read_bytes()
        if bytes.endswith('\0'):
            bytes = sample_string(bytes[0:-1], 20)
        else:
            bytes = sample_string(bytes, 20) + ' (missing NULL terminator)'
        return bytes

    def locate_end(self):
        ofs = 0

        while True:
            data = self.read_bytes(CharacterRange(ofs, ofs+4096))
            if '\0' in data:
                return data.index('\0') + ofs + 1
            elif len(data) < 4096:
                return len(data) + ofs
                break
            ofs += 4096

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

class HeteroArray(Data):
    __base_type__ = None

    def __init__(self, session, referrer, dsid):
        if self.__base_type__ is None:
            raise TypeError("HeteroArray must have __base_type__ set")

        Data.__init__(self, session, referrer, dsid)

        self.ranges = []
        self.ofs = 0
        self.last = False
        self.times_refreshed = 0
        self.invalidated_offset = None

    def is_last_item(self, datastore):
        return False

    def do_get_ranges(self, stop=None):
        last = False

        times_refreshed = -1
        ranges = None
        ofs = None

        while True:
            with self.session.lock:
                # if we got new data from a previous iteration, and some other loop
                # hasn't beat us to setting it, set it now
                if times_refreshed == self.times_refreshed:
                    self.ranges = ranges
                    self.ofs = ofs
                    self.times_refreshed += 1
                else:
                    last = False
                # if we have enough data to fill the request, return
                if (stop is not None and len(self.ranges) > stop) or self.ofs is END or last:
                    return self.ranges
                times_refreshed = self.times_refreshed
                ranges = self.ranges[:]
                ofs = self.ofs
                last = False

            # read new data
            while ((stop is None or len(ranges) <= stop) and ofs is not END and not last):
                if ranges:
                    temp_item = self.open((ranges[-1], self.__base_type__), '<temporary>')
                    try:
                        last = self.is_last_item(temp_item)
                    finally:
                        temp_item.release('<temporary>')
                    if last:
                        break

                if not self.read_bytes(CharacterRange(ofs, ofs+1)):
                    last = True
                    break

                temp_item = self.open((CharacterRange(ofs, END), self.__base_type__), '<temporary>')

                try:
                    size = temp_item.locate_end()
                    if size == 0:
                        last = True
                        break
                finally:
                    temp_item.release('<temporary>')

                if size is END:
                    ranges.append(CharacterRange(ofs, END))
                    ofs = END
                else:
                    ranges.append(CharacterRange(ofs, ofs+size))
                    ofs += size

    def get_range(self, n):
        ranges = self.do_get_ranges(n+1)
        if n >= len(ranges):
            return CharacterRange(0,0)
        return ranges[n]

    def enum_keys(self, progresscb=do_nothing):
        ranges = self.do_get_ranges(None)
        return xrange(len(ranges))

    def locate_field(self, key):
        if isinstance(key, int):
            return self.dsid + (self.get_range(key),)
        return Data.locate_field(self, key)

    def get_child_dsid(self, key):
        if isinstance(key, int):
            return (self.dsid + (key,)), self.__base_type__
        return Data.get_child_dsid(self, key)

    def locate_end(self):
        ranges = self.do_get_ranges(None)
        if ranges:
            return ranges[-1].end
        else:
            return 0

    def notify_change(self, key, requestor):
        if isinstance(key, CharacterRange):
            with self.session.lock:
                any_invalid_data = False
                # delete any invalid data
                while self.ranges and (self.ranges[-1].start >= key.start):
                    self.ofs = self.ranges[-1].start
                    self.ranges.pop(-1)
                    self.notify_change(len(ranges), requestor)
                    any_invalid_data = True
                self.times_refreshed += 1

        Data.notify_change(self, key, requestor)
        

class Structure(Data):
    def __init__(self, *args):
        Data.__init__(self, *args)

        self.fields = None
        self.times_refreshed = 0

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
        checked_bytes = set()
        times_refreshed = -1

        while True:
            with self.session.lock:
                if self.times_refreshed == times_refreshed:
                    self.fields = fields
                    self.warnings = warnings
                    self.field_order = field_order
                    self.field_data = field_data
                    self.times_refreshed += 1
                if self.fields is not None:
                    return self.fields, self.warnings, self.field_order
                times_refreshed = self.times_refreshed

            fields = {}
            warnings = []
            field_order = []
            field_data = {}

            for field in self.__fields__:
                name, klass = field[0:2]

                start = ofs
                end = None
                optional = False
                skip = False

                for i in range(2, len(field), 2):
                    setting, value = field[i:i+2]
                    if setting == 'size':
                        end = start + value
                    elif setting == 'size_is':
                        value = value.lower()
                        if value not in fields:
                            skip = True
                            break
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
                        if value not in fields:
                            skip = True
                            break
                        ref_field = fields[value]
                        if value not in field_data:
                            field_data[value] = self.read_bytes(CharacterRange(ref_field.start, ref_field.end))
                        data = field_data[value]
                        if data != expected_data:
                            skip = True
                            break
                    elif setting == 'starts_with':
                        value = value.lower()
                        if value not in fields:
                            skip = True
                            break
                        ref_field = fields[value]
                        start = ref_field.start
                    elif setting == 'ends_with':
                        value = value.lower()
                        if value not in fields:
                            skip = True
                            break
                        ref_field = fields[value]
                        end = ref_field.end
                    else:
                        raise TypeError("unknown structure field setting: %s" % setting)

                if skip:
                    continue

                if end is None:
                    temp_field = self.open((CharacterRange(start, END), klass), '<temporary>')
                    try:
                        end = temp_field.locate_end()
                        if end is not END:
                            end += start
                    except:
                        end = END
                    finally:
                        temp_field.release('<temporary>')

                if end is not END:
                    ofs = end

                if start != end:
                    if not self._check_byte(start, checked_bytes):
                        if not optional:
                            warnings.append(BrokenData('Missing field %s' % name))
                        continue
                    elif end is not END and not self._check_byte(end-1, checked_bytes):
                        warnings.append(BrokenData('Truncated field %s' % name))

                fields[name.lower()] = DataFieldInfo(name, None, klass, start, end)
                field_order.append(name)

    def notify_change(self, key, requestor):
        Data.notify_change(self, key, requestor)
        if isinstance(key, CharacterRange):
            with self.session.lock:
                # FIXME: we could possibly be smarter about this
                if self.fields is not None:
                    for field in self.__fields__:
                        self.notify_change(field[0], requestor)
                    self.times_refreshed += 1
                    self.fields = None

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
        self.changes = StreamChanges()

    def get_fd(self, writable=False):
        assert not self.session.lock._is_owned() # No blocking operations allowed while the session is locked

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
            yield CharacterRange(0, self.changes.get_size(st.st_size))
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

    def read_disk_bytes(self, r=ALL, progresscb=do_nothing):
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

    def read_bytes(self, r=ALL, progresscb=do_nothing):
        with self.lock:
            return self.changes.read_bytes(self.read_disk_bytes, self.get_size(), r, progresscb)

    def write_bytes(self, src_datastore, requestor, r=ALL, progresscb=do_nothing):
        with self.lock:
            self.changes.write_bytes(src_datastore, requestor, self.notify_change, r)
        self.set_modified()
        return [self]

    def commit(self, progresscb=do_nothing):
        raise TypeError

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

    def get_size(self):
        with self.lock:
            fd, st = self.get_fd()
            if fd is None:
                raise IOError("Not a regular file")

            return self.changes.get_size(st.st_size)

class _StreamChangesTempFile(object):
    def __init__(self):
        self.tempfile = tempfile.SpooledTemporaryFile(max_size=20480)
        self.refs = 0
        self.size = 0

    def ref(self):
        self.refs += 1

    def unref(self):
        self.refs -= 1
        if self.refs == 0:
            self.tempfile.close()

    def __enter__(self):
        self.ref()

    def __exit__(self, type, value, traceback):
        self.unref()

    def readprogress(self, part, whole, data):
        self.tempfile.write(data)
        self.size += len(data)
        return True

class StreamChange(object):
    pass

class StreamChanges(object):
    # not thread-safe!
    def __init__(self):
        c = StreamChange()
        c.data_file = None
        c.data_offset = 0
        c.len = None
        self.changes = [c]
        self.size_difference = 0

    zero_4096 = '\0' * 4096

    def write_bytes(self, src_datastore, requestor, notify_change_cb, r=ALL):
        if requestor is None:
            raise ValueError("a requestor must be specified")

        lower = 0
        upper = -1
        ofs = 0
        new_ranges = []

        for i in range(len(self.changes)):
            new_range = CharacterRange(ofs, END if self.changes[i].len is None else ofs + self.changes[i].len)
            new_ranges.append(new_range)

            if new_range.end is not END and new_range.end <= r.start:
                lower = i + 1
            if r.end is not END and new_range.start >= r.end:
                upper = i - 1
                break

            ofs = new_range.end
        else:
            upper = len(self.changes)-1

        new_tempfile = _StreamChangesTempFile()

        with new_tempfile:
            src_datastore.read_bytes(progresscb=new_tempfile.readprogress)

            new_change = StreamChange()
            new_change.len = new_tempfile.size
            new_change.data_file = new_tempfile
            new_change.data_offset = 0

            if upper >= lower:
                if r.end is not END and (new_ranges[upper].end is END or new_ranges[upper].end > r.end):
                    new_upper_change = StreamChange()
                    if self.changes[upper].len is None:
                        new_upper_change.len = None
                    else:
                        new_upper_change.len = self.changes[upper].len + new_ranges[upper].start - r.end
                    new_upper_change.data_file = self.changes[upper].data_file
                    new_upper_change.data_offset = self.changes[upper].data_offset + r.end - new_ranges[upper].start
                    self.changes.insert(upper+1, new_upper_change)
                    if new_upper_change.len is not None:
                        self.size_difference += new_upper_change.len
                    if new_upper_change.data_file is not None:
                        new_upper_change.data_file.ref()

                if r.start > new_ranges[lower].start:
                    new_lower_change = StreamChange()
                    new_lower_change.len = r.start - new_ranges[lower].start
                    new_lower_change.data_file = self.changes[lower].data_file
                    new_lower_change.data_offset = self.changes[lower].data_offset
                    if self.changes[lower].len is not None:
                        self.size_difference += new_lower_change.len - self.changes[lower].len
                    self.changes[lower] = new_lower_change
                    if new_lower_change.data_file is not None and new_lower_change.data_file.refs == 1:
                        new_lower_change.data_file.tempfile.truncate(new_lower_change.data_offset + new_lower_change.len)
                    lower += 1

                if upper >= lower:
                    deleted_changes = self.changes[lower:upper+1]
                    self.changes[lower:upper+1] = ()
                    for change in deleted_changes:
                        if change.len is not None:
                            self.size_difference -= change.len
                        if change.data_file is not None:
                            change.data_file.unref()

            if new_tempfile.size != 0:
                self.changes.insert(lower, new_change)
                new_change.data_file.ref()
                self.size_difference += new_tempfile.size

            if r.end is END or new_change.len == r.end - r.start:
                notify_change_cb(r, requestor)
            else:
                notify_change_cb(CharacterRange(r.start, END), requestor)

    def get_size(self, orig_size):
        if self.changes and self.changes[-1].len is None:
            return self.size_difference + max(0, orig_size - self.changes[-1].data_offset)
        else:
            return self.size_difference

    def read_bytes(self, read_orig_bytes_cb, orig_size, r=ALL, progresscb=do_nothing):
        if r.end == r.start:
            return ''

        ofs = 0
        read_index = r.start
        result = []
        bytes_read = [0]

        if r.end is END:
            r = CharacterRange(r.start, self.get_size(orig_size))

        def my_progresscb(part, whole, data):
            bytes_read[0] += len(data)
            if not progresscb(bytes_read[0], r.end - r.start, data):
                result.append(data)
            return True

        for change in self.changes:
            if ofs >= r.end:
                break

            if change.len is None:
                change_len = orig_size - change.data_offset
                if change_len <= 0:
                    break
            else:
                change_len = change.len

            if r.start <= ofs + change_len:
                segment_start = max(0, r.start - ofs)
                segment_end = min(change_len, r.end - ofs)

                if change.data_file is None:
                    read_orig_bytes_cb(CharacterRange(segment_start + change.data_offset, segment_end + change.data_offset), my_progresscb)
                    if bytes_read[0] + r.start < segment_end:
                        zeros_to_return = segment_end - bytes_read[0] - r.start
                        zero_blocks, zeros_to_return = divmod(zeros_to_return, 4096)
                        for i in range(zero_blocks):
                            my_progresscb(None, None, StreamChanges.zero_4096)
                        if zeros_to_return:
                            my_progresscb(None, None, '\0' * zeros_to_return)
                else:
                    change.data_file.tempfile.seek(segment_start + change.data_offset)
                    while bytes_read[0] + r.start - ofs < segment_end:
                        my_progresscb(None, None, change.data_file.tempfile.read(min(4096, segment_end - bytes_read[0] - r.start)))

            ofs += change_len

        return ''.join(result)

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

