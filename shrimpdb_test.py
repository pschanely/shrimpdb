import os
import random
import unittest
import tempfile

import shrimpdb

class ShrimpDbTestCase(unittest.TestCase):

    def test_shrimpdb(self):
        fh = tempfile.NamedTemporaryFile()
        fh.close()
        try:
            db = shrimpdb.ShrimpDb(filename=fh.name)
            self.assertEquals({}, db.view())
            with db as root:
                root['people'] = ['Jim','Phil']
                root['score'] = {'top':8}
            self.assertEquals({'people':['Jim','Phil'], 'score':{'top':8}}, db.view())

            with db as root:
                root['people'].pop()
                root['score2'] = root['score']
            self.assertEquals({'people':['Jim'], 'score':{'top':8}, 'score2':{'top':8}}, db.view())

            orig_view = db.view()
            orig_addr = orig_view['score']._addr
            orig_addr2 = orig_view['score2']._addr
            with db as root:
                # it's ok to re-use data from a different view (so long as it's from the same file):
                root['score2'] = orig_view['score']
                # you can also re-write a new version of the same data
                root['score'] = {'top':8}
            self.assertEquals(orig_addr, db.view()['score2']._addr)
            # even if we wrote new data, we still re-use the serialized verions, so long as the data is the same:
            self.assertEquals(orig_addr, db.view()['score']._addr)

            old_size = db.size()
            db = db.compact()
            self.assertTrue(db.size() < old_size)
            self.assertEquals({'people':['Jim'], 'score':{'top':8}, 'score2':{'top':8}}, db.view())
        finally:
            os.unlink(fh.name)

    def test_shrimpdb(self):
        fh = tempfile.NamedTemporaryFile()
        fh.close()
        NUM_BATCHES, BATCH_SIZE, TREE_DEPTH = 100, 100, 4
        # NUM_BATCHES, BATCH_SIZE, TREE_DEPTH = 500, 2000, 4
        try:
            db = shrimpdb.ShrimpDb(filename=fh.name)
            alphabet = [chr(ord('a')+i) for i in range(26)]
            def generator():
                r = random.Random(1111111)
                while True:
                    key = ''.join([r.choice(alphabet) for _ in range(16)])
                    val = {'name':'Phillip','age':33, 
                           'description':'Fluffy.  Really fluffy.  Like fluffirific.'}
                    yield key, val

            gen1 = generator()
            for _ in xrange(NUM_BATCHES):
                with db as root:
                    for _ in xrange(BATCH_SIZE):
                        key, val = gen1.next()
                        pointer = root
                        for idx in range(TREE_DEPTH+1):
                            if key[idx] not in pointer:
                                pointer[key[idx]] = {}
                            pointer = pointer[key[idx]]
                        pointer[key[TREE_DEPTH:]] = val
                print ' ----------- ', db.size()

            print 'compating ', db.size()
            db = db.compact()
            print 'compated  ', db.size()

            gen2 = generator()
            view = db.view()
            for _ in xrange(NUM_BATCHES):
                for _ in xrange(BATCH_SIZE):
                    k, val = gen2.next()
                    pointer = view
                    for idx in range(TREE_DEPTH+1):
                        pointer = pointer[k[idx]]
                    self.assertEquals(pointer[k[TREE_DEPTH:]], val)
        finally:
            os.unlink(fh.name)

if __name__ == '__main__':
    unittest.main()


