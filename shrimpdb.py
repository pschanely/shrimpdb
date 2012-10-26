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
import copy
import os
import threading
import UserDict
import weakref

try:
    import ujson as json
except ImportError:
    import json

def _resolve_addrs(obj, view):
    if isinstance(obj, basestring):
        if obj[0] == "|":
            return obj[1:]
        else:
            addr = int(obj, 16)
            return view.get(addr)
    elif hasattr(obj, '__iter__'):
        return [_resolve_addrs(item, view) for item in obj]
    else:
        return obj

_NOVAL = object()

class ShrimpDb(object):
    def __init__(self, filename, view_wrapper=lambda x:x):
        self.filename = filename
        self.view_wrapper = view_wrapper
        if not os.path.exists(filename):
            open(filename, 'wb').close()
        self.opendb()
        self.update_lock = threading.Lock()
        self.fh_lock = threading.RLock()
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

    def drop(self):
        self.closedb()
        os.unlink(self.filename)

    def view(self):
        return self.view_wrapper(DbView(
                self, self.root_pointer, is_weak=True).get())

    def size(self):
        with self.fh_lock:
            self.fh.seek(0, 2)
            return self.fh.tell()

    def compact(self):
        with self.update_lock:
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
                obj._view.shrimp_db.fh.fileno() == self.fh.fileno())

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

    def readline(self, addr):
        with self.fh_lock:
            fh = self.fh
            fh.seek(addr)
            return json.loads(fh.readline()[:-1])

    def writeline(self, data):
        with self.fh_lock:
            fh = self.fh
            fh.seek(0, 2)
            addr = fh.tell()
            fh.write(json.dumps(data))
            fh.write('\n')
            return addr

    def write_changes(self, newroot):
        root, same = self.compare_and_write(
            DbView(self, self.root_pointer, is_weak=True).get(),
            newroot)
        if not same:
            self.root_pointer = int(root, 16)
            with self.fh_lock:
                fh = self.fh
                fh.seek(0)
                fh.write('%08x' % self.root_pointer)
                fh.flush()
                os.fsync(fh.fileno())

    def __enter__(self):
        self.update_lock.acquire()
        if self.current_write_view is not None:
            raise Exception('Cannot update inside another update')
        self.current_write_view = DbView(self, self.root_pointer, is_weak=False)
        return self.view_wrapper(self.current_write_view.get())

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.write_changes(self.current_write_view.get())
        self.current_write_view = None
        self.update_lock.release()

class DbView(object):
    def __init__(self, shrimp_db, root_addr, is_weak=True):
        self.shrimp_db = shrimp_db
        self.cache = weakref.WeakValueDictionary() if is_weak else {}
        self.is_weak = is_weak
        self.root_addr = root_addr

    def get(self, addr=None):
        if addr is None:
            addr = self.root_addr
        cached = self.cache.get(addr)
        if cached:
            return cached
        val = ShrimpDict(self, addr, self.is_weak)
        self.cache[addr] = val
        return val

class WeakableDict(dict):
    pass  # because native dicts can't be weakref'd!
        
class ShrimpDict(UserDict.DictMixin, 
                 # these supclasses are here just because common libraries test for them:
                 collections.MutableMapping, dict 
                 ):

    def __init__(self, view, addr, is_weak):
        self._view = view
        self._addr = addr
        self._state = lambda :None
        self._is_weak = is_weak

    def copy(self):
        return dict(self.iteritems())

    def __deepcopy__(self, memo):
        return dict((k, copy.deepcopy(v, memo)) for k, v in self.iteritems())

    def _materialize(self):
        state = self._state()
        if state is None:
            obj = self._view.shrimp_db.readline(self._addr)
            state = WeakableDict((k, _resolve_addrs(v, self._view))
                                 for k, v in obj.iteritems())
            self._state = weakref.ref(state) if self._is_weak else lambda :state
        return state
            
    def __delitem__(self, key):
        return self._materialize().__delitem__(key)
        
    def __setitem__(self, key, val):
        return self._materialize().__setitem__(key, val)
    
    def __getitem__(self, key):
        return self._materialize().__getitem__(key)

    def keys(self):
        return self._materialize().keys()

    def __contains__(self, key):
        return self._materialize().__contains__(key)

    def __iter__(self):
        return self._materialize().__iter__()

    def iteritems(self):
        return self._materialize().iteritems()

    def items(self):
        return self._materialize().items()

    def iterkeys(self):
        return self._materialize().iterkeys()
