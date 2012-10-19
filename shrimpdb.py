"""
shrimpdb.py - A tiny, simple, and capable embedded database for Python

Copyright 2012 Phillip Schanely

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import collections
import json
import os
import UserDict
import threading
import weakref

def _resolve_addrs(obj, view):
    if isinstance(obj, basestring):
        if obj[0] == "|":
            return obj[1:]
        else:
            addr = int(obj, 16)
            return ShrimpDict(view, addr)
    elif hasattr(obj, '__iter__'):
        return [_resolve_addrs(item, view) for item in obj]
    else:
        return obj

_NOVAL = object()

class ShrimpDb(object):
    def __init__(self, filename):
        self.filename = filename
        if not os.path.exists(filename):
            open(filename, 'wb').close()
        self.opendb()
        self.lock = threading.RLock()
        self.current_write_view = None
    
    def opendb(self):
        self.fh = fh = open(self.filename, 'r+b')
        fh.seek(0, 2)
        if fh.tell() == 0:
            fh.write('%08x\n{}\n' % 9)
        fh.seek(0)
        self.root_pointer = int(fh.read(8), 16)

    def closedb(self):
        self.fh.close()
        self.fh = None

    def view(self):
        return DbView(self, self.root_pointer).get()

    def size(self):
        with self.lock:
            self.fh.seek(0, 2)
            return self.fh.tell()

    def compact(self):
        with self.lock:
            tempfilename = self.filename+'.compacting'
            tmpdb = ShrimpDb(filename=tempfilename)
            tmpdb.write_changes(self.view())
            tmpdb.closedb()
            self.closedb()
            os.unlink(self.filename)
            os.rename(tempfilename, self.filename)
            return ShrimpDb(self.filename)

    def is_db_dict(self, obj):
        return (isinstance(obj, ShrimpDict) and 
                obj._view.sync_db.fh.fileno() == self.fh.fileno())

    def compare_and_write(self, oldobj, newobj):
        all_same = True
        if isinstance(newobj, collections.Mapping):
            if self.is_db_dict(newobj):
                if not newobj._state != None: # it's materialized
                    if self.is_db_dict(oldobj):
                        return hex(newobj._addr), True
                    else:
                        return hex(newobj._addr), False
            if not isinstance(oldobj, collections.Mapping):
                oldobj = {}
                all_same = False
            if len(oldobj) != len(newobj):
                all_same = False
            result = {}
            for k, newv in newobj.iteritems():
                oldv = oldobj.get(k, _NOVAL)
                result[k], same = self.compare_and_write(oldv, newv)
                all_same = all_same and same
            if all_same and self.is_db_dict(oldobj):
                # materialized ... but nothing changed, so leave it
                return hex(oldobj._addr), True
            else:
                addr = self.writeline(result)
                return hex(addr), all_same
        elif isinstance(newobj, basestring):
            return '|' + newobj, oldobj == newobj
        elif hasattr(newobj, '__iter__'):
            if not hasattr(oldobj, '__iter__'):
                all_same = False
                oldobj = []
            result = []
            oldlen = len(oldobj)
            if oldlen != len(newobj):
                all_same = False
            for idx, newv in enumerate(newobj):
                if idx < oldlen:
                    oldv = oldobj[idx]
                else:
                    oldv = None
                item, same = self.compare_and_write(oldv, newv)
                all_same = all_same and same
                result.append(item)
            return result, all_same
        else:
            return newobj, oldobj == newobj

    def writeline(self, data):
        fh = self.fh
        fh.seek(0, 2)
        addr = fh.tell()
        fh.write(json.dumps(data))
        fh.write('\n')
        return addr

    def write_changes(self, newroot):
        root, same = self.compare_and_write(
            DbView(self, self.root_pointer).get(),
            newroot)
        if not same:
            self.root_pointer = int(root, 16)
            fh = self.fh
            fh.seek(0)
            fh.write('%08x' % self.root_pointer)
            fh.flush()
            os.fsync(fh.fileno())

    def __enter__(self):
        self.lock.acquire()
        if self.current_write_view is not None:
            raise Exception('Cannot update inside another update')
        self.current_write_view = DbView(self, self.root_pointer)
        return self.current_write_view.get()

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.write_changes(self.current_write_view.get())
        self.current_write_view = None
        self.lock.release()

class DbView(object):
    def __init__(self, sync_db, root_addr):
        self.sync_db = sync_db
        self.cache = weakref.WeakValueDictionary()
        self.root_addr = root_addr

    def readline(self, addr):
        with self.sync_db.lock:
            fh = self.sync_db.fh
            fh.seek(addr)
            return json.loads(fh.readline())

    def get(self, addr=None):
        if addr is None:
            addr = self.root_addr
        cached = self.cache.get(addr)
        if cached:
            return cached
        val = ShrimpDict(self, addr)
        self.cache[addr] = val
        return val
        
class ShrimpDict(UserDict.DictMixin, collections.MutableMapping):

    def __init__(self, view, addr):
        self._view = view
        self._addr = addr
        self._state = None

    def _materialize(self):
        if self._state is None:
            state = self._view.readline(self._addr)
            self._state = dict((k, _resolve_addrs(v, self._view))
                               for k, v in state.iteritems())
            
    def __delitem__(self, key):
        self._materialize()
        return self._state.__delitem__(key)
        
    def __setitem__(self, key, val):
        self._materialize()
        return self._state.__setitem__(key, val)
    
    def __getitem__(self, key):
        self._materialize()
        return self._state.__getitem__(key)

    def keys(self):
        self._materialize()
        return self._state.keys()

    def __contains__(self, key):
        self._materialize()
        return self._state.__contains__(key)

    def __iter__(self):
        self._materialize()
        return self._state.__iter__()

    def iteritems(self):
        self._materialize()
        return self._state.iteritems()
