shrimpdb
========

ShrimpDB is a tiny yet capable embedded database for Python.  Insofar as databases can be "cute", ShrimpDB is.

ShrimpDB maintains a large tree of JSON-compatible Python objects in an append-only file.  It's just a few hundred lines of forkable goodness.

Use "with db" to make modifications (changes is the block are committed atomically):

            import shrimpdb
            db = shrimpdb.ShrimpDb(filename='/tmp/test.shrimpdb')
            with db as root:
                root['people'] = {'Phillip': {'age':34}, 'Tyler':{'age':5}}
                root['colors'] = ['red', 'green', 'blue']

Use db.view() for read-only access:

            view = db.view()
            print view['people']['Phillip']['age']

ShrimpDb is thread safe.  The dictionaries returned are always lazily instantiated, so your structure on disk can be very large.  When modifying the structure, you may use all the usual list and dictionary methods:

            with db as root:
                root['people']['Phillip']['children'] = ['Tyler']
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
            {"Phillip": "0x17", "Tyler": "0xc"}
            {"colors": ["|red", "|green", "|blue"], "people": "0x23"}

ShrimpDB in practice
--------------------

If you need secondary indicies, you'll need to maintain them yourself, like so:

            with db as root:
                root['people']['Vivian'] = {'age':7}
                root['people_by_age'][7].append('Vivian')

Because data is loaded one-dictionary-at-a-time, instead of having very large dictionaries you should split up the key into parts and nest additional dictionaries.  For example, we might change the above examples to make the first two characters of the key each have their own dictionary, like so:

            view = db.view()
            print view['people']['P']['h']['illip']
