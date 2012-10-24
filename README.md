shrimpdb
========

A tiny, simple, and capable embedded database for Python


Append-only database that can maintain a large tree of JSON-compatible Python objects on disk.  It's just a few hundred lines of forkable goodness.

Use "with db" to make modifications (these changes are committed atomically):

            import shrimpdb
            db = shrimpdb.ShrimpDb(filename='/tmp/test.shrimpdb')
            with db as root:
                root['people'] = {'Phil': {'age':34}, 'Tyler':{'age':5}}
                root['colors'] = ['red', 'green', 'blue']

Use db.view() for read-only access:

            view = db.view()
            print view['people']['Phil']['age']

ShripDb is thread safe.
The dictionaries returned are always lazily instantiated, so your structure on disk can be much larger than RAM.  When modifying the structure, you may use all the usual list and dictionary methods:

            with db as root:
                root['people']['Phil']['children'] = ['Tyler']
                root['colors'].pop()

The structure should be a tree, not an arbitrary graph (no cycles, and there should be only one path to reach any node).  If an update does not modify anything under a certain dictionary, it will re-use the previously serialized version of that dictionary, writing only the structure that rests above the changes.

Because it's append-only, old db.view()s never change.  Feel free to hold on and use as many as you like.  That is, until you compact the database, which will remove any data that's not reachable from the current root:

            db = db.compact()

The database is human-readable, with one line per json-encoded dictionary and file offset pointers in hex to sub-dictionaries.  For instance, the first update above serializes like so:

            $ cat /tmp/test.shrimpdb
            00000044
            {}
            {"age": 5}
            {"age": 34}
            {"Phil": "0x17", "Tyler": "0xc"}
            {"colors": ["|red", "|green", "|blue"], "people": "0x23"}

Because data is loaded one-dictionary-at-a-time, instead of having very large dictionaries you should split up the key into parts and nest additional dictionaries.  For instance, we might make the first two characters of the key each have their own dictionary, like so:

            view = db.view()
            print view['people']['P']['h']['il']['age']
