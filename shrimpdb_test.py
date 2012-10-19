import os
import unittest
import tempfile

import shrimpdb

class CoreServerTestCase(unittest.TestCase):

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
            orig_addr = db.view()['score']._addr
            with db as root:
                root['people'].pop()
                root['score'] = {'top':8} # re-write the same thing
            self.assertEquals({'people':['Jim'], 'score':{'top':8}}, db.view())
            self.assertEquals(orig_addr, db.view()['score']._addr) # unchanged
            old_size = db.size()
            db = db.compact()
            self.assertTrue(db.size() < old_size)
            self.assertEquals({'people':['Jim'], 'score':{'top':8}}, db.view())
        finally:
            os.unlink(fh.name)

if __name__ == '__main__':
    unittest.main()


