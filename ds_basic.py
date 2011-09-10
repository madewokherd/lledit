
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

def do_nothing(*args, **kwargs):
    pass

class Session(object):
    def __init__(self):
        self.open_datastores = {}
        self.lock = threading.RLock()

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

                    while i > 0:
                        if dsid[0:i] in self.open_datastores:
                            result = self.open_datastores[dsid[0:i]]
                            break
                        i -= 1

                if i == 0:
                    intermediate_dsid = (dsid[0],)
                    with self.lock:
                        if intermediate_dsid not in self.open_datastores:
                            self.open_datastores[intermediate_dsid] = roots[dsid[0]](self, '<temporary>', intermediate_dsid)
                            to_release.append(self.open_datastores[intermediate_dsid])
                elif i < len(dsid):
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

    def enum_keys(self):
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

    def get_parent_dsid(self):
        if len(self.dsid) == 1:
            return None
        return self.dsid[0:-1]

class Slice(DataStore):
    def __init__(self, session, referrer, dsid):
        DataStore.__init__(self, session, referrer, dsid)

        self.parent = self.get_datastore(dsid[0:-1])
        self.range = dsid[-1]

    def translate_range(self, r):
        start = r.start + self.range.start

        if r.end is not END:
            end = r.end + self.range.start

            if self.range.end is not END:
                end = min(self.range.end, end)
        else:
            end = r.end

        return CharacterRange(start, end)

    def enum_keys(self):
        return CharacterRange(0, END if self.range.end is END else self.range.end - self.range.start)

    def read_bytes(self, r=ALL, progresscb=do_nothing):
        return self.parent.read_bytes(self.translate_range(r), progresscb)

    def get_child_dsid(self, key):
        if isinstance(key, CharacterRange):
            return (self.parent.dsid + (self.translate_range(key),)), Slice
        else:
            return DataStore.get_child_dsid(self, key)

class FileSystemObject(DataStore):
    __ds_rootname__ = "FileSystem"

    def __init__(self, session, referrer, dsid):
        DataStore.__init__(self, session, referrer, dsid)
        if len(dsid) == 1:
            path = None
        else:
            path = dsid[-1]
        if path is not None:
            path = os.path.normpath(path)        
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
                key = os.path.join(self.path, key)
            if key == '/':
                return ('FileSystem',), FileSystemObject
            else:
                return ('FileSystem', key), FileSystemObject
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
                    bytestoread = max(4096, r.end - offset)
                last_res = os.read(self.fd, bytestoread)
                if last_res:
                    offset += len(last_res)
                    if not progresscb(offset - r.start, expected_end - offset, last_res):
                        result.append(last_res)

        return ''.join(result)

    def get_parent_dsid(self):
        if self.path is None or self.path == '/':
            return DataStore.get_parent_dsid(self)
        path = os.path.dirname(self.path)
        if os.path.sep == '\\' and path == self.path:
            # drive root
        

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

def get_dsid(datastore):
    path_elements = []
    while datastore is not None:
        datastore, key = datastore.get_parent()
        if key is None:
            return None
        path_elements.insert(0, key)
    return tuple(path_elements)

# FIXME
roots = {
    'FileSystem': FileSystemObject,
    }

