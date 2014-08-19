__author__ = 'jerryj'
__update__ = 'duynguyen'

import unittest
import arithmetic
from arithmetic import sum_cols, merge, merge_insert_tombstone, \
    break_down_cols_by_group, \
    merge_delete_record_with_valid_tombstone, \
    merge_insert_merge, merge_delete_invalid, incre, \
    get_current_timeuuid, get_new_version \

class MyTestCase(unittest.TestCase):


    def setUp(self):
        print '****Start ', self._testMethodName

    def tearDown(self):
        print '****Finish', self._testMethodName


    def test_merge(self):
        cols = convert_to_cf_tuple([1, 5])
        merge(cols)
        assert len(cols) == 5
        merge(cols)
        assert len(cols) == 3
        merge(cols)
        assert len(cols) == 3
        
    def test_incre_merge_overlap(self):
        cols = convert_to_cf_tuple([1, 5])
        t1 = TestHelper("m1")
        t2 = TestHelper("a1")
        t1.read_data(cols)
        t2.read_data(cols)
        t1.insert_tombstone()
        t2.incre(1)
        t1.insert_merge()
        t1.refresh()
        t1.delete_with_valid_tombstone()
        t1.delete_with_invalid_tombstone()
        assert sum_cols(cols) == 7


    def test_merge_1122(self):
        '''
        m1.start
        m1.finish
        m2.start
        m2.finish
        :return:
        '''
        cols = convert_to_cf_tuple([1, 5])
        m1 = TestHelper("m1")
        m2 = TestHelper("m2")
        m1.read_data(cols)
        m1.insert_tombstone()
        m1.insert_merge()
        m1.delete_with_valid_tombstone()
        m1.delete_with_invalid_tombstone()

        m2.read_data(cols)
        m2.insert_tombstone()
        m2.insert_merge()
        m2.delete_with_valid_tombstone()
        m2.delete_with_invalid_tombstone()
        assert sum_cols(cols, '1122') == 6

        m1.read_data(cols)
        m1.merge()
        m2.read_data(cols)
        m2.merge()
        assert sum_cols(cols, '1122') == 6


    def test_merge_1221(self):
        '''
        m1.start
        m2.start
        m2.finish
        m1.finish
        :return:
        '''
        cols = convert_to_cf_tuple([1, 5])
        m1 = TestHelper("m1")
        m2 = TestHelper("m2")
        m1.read_data(cols)
        m2.read_data(cols)

        m1.insert_tombstone()
        m2.insert_tombstone()
        m2.insert_merge()
        m1.insert_merge()

        m1.delete_with_valid_tombstone()
        m1.delete_with_invalid_tombstone()

        m2.delete_with_valid_tombstone()
        m2.delete_with_invalid_tombstone()
        assert sum_cols(cols, '1221') == 6

    def test_merge_1212(self):
        '''
        m1.start
        m2.start
        m1.finish
        m2.finish

        :return:
        '''
        cols = convert_to_cf_tuple([1, 5])
        m1 = TestHelper("m1")
        m2 = TestHelper("m2")
        m1.read_data(cols)
        m2.read_data(cols)

        m1.insert_tombstone()
        m2.insert_tombstone()
        m1.insert_merge()
        m2.insert_merge()

        m1.delete_with_valid_tombstone()
        m1.delete_with_invalid_tombstone()

        m2.delete_with_valid_tombstone()
        m2.delete_with_invalid_tombstone()
        assert sum_cols(cols, '1212') == 6

    def test_merge_1212_incre(self):
        '''
        m1.start
        m2.start
        m1.finish
        m2.finish

        :return:
        '''
        cols = convert_to_cf_tuple([1, 5])
        m1 = TestHelper("m1")
        m2 = TestHelper("m2")
        m1.read_data(cols)
        m2.read_data(cols)

        m1.insert_tombstone()
        m2.insert_tombstone()
        m1.insert_merge()
        m1.incre(1)
        m2.insert_merge()



        m1.delete_with_valid_tombstone()
        m1.delete_with_invalid_tombstone()

        m2.delete_with_valid_tombstone()
        m2.delete_with_invalid_tombstone()
        assert sum_cols(cols, '1212') == 7

    def test_merge_121332(self):
        '''
        m1.start
        m2.start
        m2.read_data
        m1.finish
        insert 2
        m3.start
        m3.finish
        m2.finish
        :return:
        '''
        cols = convert_to_cf_tuple([1, 5])
        m1 = TestHelper("m1")
        m2 = TestHelper("m2")
        m3 = TestHelper("m3")
        
        m1.read_data(cols)
        m2.read_data(cols)
        
        
        
        m1.insert_tombstone()
        m1.insert_merge()
        m1.delete_with_valid_tombstone()
        m1.delete_with_invalid_tombstone()
        
        m1.incre(2)
        
        m3.read_data(cols)
        m3.insert_tombstone()
        m3.insert_merge()
        m3.delete_with_valid_tombstone()
        m3.delete_with_invalid_tombstone()
        
        assert sum_cols(cols, '121332') == 8
        
        m2.insert_tombstone()
        m2.insert_merge()
        m2.delete_with_valid_tombstone()
        m2.delete_with_invalid_tombstone()
        m1.incre(2)
        assert sum_cols(cols, '121332') == 10

    def test_sum(self):
        cols = [(3,'S',1,9),(3,'T',2,9),(4,'N',None,1),(4,'T',2,1)]
        total = sum_cols(cols)
        assert total == 10
        assert sum_cols(convert_to_cf_tuple([1,3,5])) == 9

    def test_convert(self):
        assert convert_to_cf_tuple([1,3,5]) == [(0,'N',None,1),(1,'N',None,3),(2,'N',None,5)]

def convert_to_cf_tuple(nums):
    cols = []
    idx = 0
    for num in nums:
        cols.append((idx, 'N', None, num))
        idx += 1
    return cols

class TestHelper(object):
    def __init__(self, name):
        self.name = name


    def read_data(self, cols):
        self.cols = cols
        self.__load()

    def refresh(self):
        print '{0}: refresh from cass'.format(self.name)
        self.__load()


    def __load(self):
        self.snapshot = list(self.cols)
        valid_cols, invalid_tombstones, valid_tombstones, lastest_update_id, first_is_merge, lastest_merge_update_id  = break_down_cols_by_group(self.snapshot)
        self.valid_cols = valid_cols
        self.invalid_tombstones = invalid_tombstones
        self.valid_tombstones = valid_tombstones
        self.new_update_id = get_current_timeuuid(self.snapshot)
        self.new_version = get_new_version()
        self.lastest_update_id = lastest_update_id
        self.first_is_merge = first_is_merge 
        self.lastest_merge_update_id = lastest_merge_update_id
        arithmetic.print_cols(self.snapshot, self.cols, self.name)

    def merge(self):
        self.insert_tombstone()
        self.insert_merge()
        self.delete_with_valid_tombstone()
        self.delete_with_invalid_tombstone()

    def insert_tombstone(self):
        merge_insert_tombstone(self.snapshot, self.cols, self.new_version, self.valid_cols,
                               self.name)

    def insert_merge(self):
        merge_insert_merge(self.snapshot, self.cols, self.new_version, self.new_update_id,
                           self.valid_cols, self.lastest_merge_update_id,
                           self.name)

    def delete_with_valid_tombstone(self):
        merge_delete_record_with_valid_tombstone(self.snapshot, self.cols, self.valid_cols,
                                                 self.name)

    def delete_with_invalid_tombstone(self):
        merge_delete_invalid(self.snapshot, self.cols, self.invalid_tombstones, self.new_update_id,
                             self.name)

    def sum(self):
        return sum_cols(self.cols, self.name)

    def incre(self, amount):
        incre(self.cols, amount)
        print '{0}: After incre {1}'.format(self.name, amount)
        arithmetic.print_cols(None, self.cols, self.name)



if __name__ == '__main__':
   unittest.main()
