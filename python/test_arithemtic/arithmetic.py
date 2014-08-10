__author__ = 'jerryj'





def incre(cols, amount):
    new_update_id = get_current_timeuuid(cols)
    cols.append((new_update_id, 'N', None, amount))

def merge(cols, log_prefix=''):
    # snapshot is memory
    #cols is what read from cassandra, but in this test case, it is just memory also
    #cols is what insert, delete are send to, however computation are using snapshot

    snapshot = list(cols)
    valid_cols, invalid_tombstones, valid_tombstones = break_down_cols_by_group(snapshot)

    new_update_id = get_current_timeuuid(snapshot)
    new_version = get_new_version()
    print '{0}: Start Merge {1}'.format(log_prefix, new_version)
    print_cols(snapshot, cols, log_prefix)

    merge_insert_tombstone(snapshot, cols, new_version, valid_tombstones, log_prefix)

    merge_insert_merge(snapshot, cols, new_version, new_update_id, valid_cols, log_prefix)

    merge_delete_record_with_valid_tombstone(snapshot, cols, valid_cols, log_prefix)

    merge_delete_invalid(snapshot, cols, invalid_tombstones, new_update_id, log_prefix)

    print '{0}: Complete Merge {1}'.format(log_prefix, new_version)
    print_cols(snapshot, cols, log_prefix)


def sum_cols(cols, log_prefix = ''):
    print '{0}: Sum {1}'.format(log_prefix, __cols_to_str(cols))
    valid_cols = break_down_cols_by_group(cols)[0]
    return __sum(valid_cols, log_prefix)


def merge_insert_tombstone(snapshot, cols, new_version, valid_tombstones, log_prefix=''):
    # Do not insert record for a N, S that already have a valid tombstone
    if snapshot[-1][1] == 'S':
        print '{0}: Skip insert tombstone because no new record inserted'.format(log_prefix)
        return

    item_to_insert = []
    cols_len = len(cols)
    for i in range(0, cols_len):
        col = cols[i]
        type = col[1]
        if type != 'T':
            if is_record_tombstoned(col, valid_tombstones):
                print '{0}: Skip insert tombstone for {0} because it already have one'.format(
                    log_prefix, col)
                continue

            tombstone = (col[0], 'T', new_version, col[3])
            inserted = False
            if i < cols_len - 1:
                for j in range(i + 1, cols_len):
                    if cols[j][0] > col[0]:
                        #insert index
                        item_to_insert.append((j, tombstone))
                        inserted = True
                        break
            if not inserted:
                item_to_insert.append((i + 1, tombstone))
    for idx, tombstone in reversed(item_to_insert):
        cols.insert(idx, tombstone)

    print '{0}: After insert tombstone '.format(log_prefix,
                                                __cols_to_str([x[1] for x in item_to_insert]))
    # print 'After insert tombstone ' + str(item_to_insert)
    print_cols(snapshot, cols, log_prefix)


def merge_insert_merge(snapshot, cols, new_version, new_updateid, valid_cols, log_prefix=''):
    if snapshot[-1][1] == 'S':
        'Skip insert tombstone because no new record inserted'
        return

    total = __sum(valid_cols, log_prefix)
    merge_record = (new_updateid, 'S', new_version, total)
    cols.append(merge_record)
    print '{0}: After insert merge {1}'.format(log_prefix, __cols_to_str([merge_record]))
    print_cols(snapshot, cols, log_prefix)


def merge_delete_record_with_valid_tombstone(snapshot, cols, valid_cols, log_prefix=''):
    del_cols = []
    for valid in valid_cols:
        if valid[1] == 'T':
            # delete normal record
            try:
                col = ((valid[0]), 'N', None, valid[3])
                cols.remove(col)
                del_cols.append(col)
            except:
                pass
            #delete merge record
            try:
                col = ((valid[0]), 'S', valid[2], valid[3])
                cols.remove(col)
                del_cols.append(col)
            except:
                pass
    if len(del_cols) == 0:
        print '{0}: Skip delete normal, merge with valid tombstone'.format(log_prefix)
        return False
    else:
        print '{0}: After delete normal, merge with valid tombstone {1}'.format(log_prefix,
                                                                                __cols_to_str(
                                                                                    del_cols))
        print_cols(snapshot, cols, log_prefix)
        return True


def merge_delete_invalid(snapshot, cols, invalids, new_update_id, log_prefix=''):
    '''
    Delete invalid record from cols(cassandra) that is computed from snapshot(memory)
    It is very important to not delete recent invalid record since another thread could have inserted it and
    they have not yet issue the insert merge record
    :param snapshot:
    :param cols:
    :param invalids:
    :return:
    '''
    if len(invalids) == 0:
        print '{0}: Skip delete invalid, nothing to delete'.format(log_prefix)
        return False
    else:
        deleted = []
        skipped = []
        for invalid in invalids:
            if invalid[0] < new_update_id - 3:
                cols.remove(invalid)
                deleted.append(invalid)
            else:
                skipped.append(invalid)
        print '{0}: After delete invalid tombstone {1}'.format(log_prefix, __cols_to_str(deleted))
        if len(skipped) > 0:
            print '{0}: Some invalid tombstone {1} are skipped because they are not old enough'.format(
                log_prefix, __cols_to_str(skipped))
        print_cols(snapshot, cols, log_prefix)
        return True


def is_record_tombstoned(r, valid_tombstones):
    '''
    Normal record is tombstoned if
        1. Exist a valid tombstone record with same updateid
            or in another word a tombstone record with same updateid and a merge record with same version as that tombstone
    Merge record is tombstoned if
        a. Exist a tombstone record for it with same updateid and a merge record with same version as that tombstone
    '''
    assert r[1] in ('N', 'S')
    for t in valid_tombstones:
        if r[0] == t[0] and r[3] == t[3]:
            return True
    return False


def break_down_cols_by_group(cols):
    '''

    break down cols in filtered, invalid tombstone, valid_tombstone
    where filtered is normal + merge + valid tombstone
    :param cols:
    :return:
    '''
    version_keys = {}
    update_id_keys = {}
    filtered = []
    valid_tombstone = []
    invalid_tombstone = []

    # generate keys first for faster lookup
    for update_id, type, version, value in cols:
        version_keys[(version, type)] = (update_id, type, version, value)
        update_id_keys[(update_id, type)] = (update_id, type, version, value)

    for col in cols:
        type = col[1]
        if type == 'T':
            '''
            check for valid tombstone
            match merge record with same version
            match normal or merge record with the same updateid
            '''
            match_merge_version = (col[2], 'S') in version_keys
            match_update_id = (col[0], 'N') in update_id_keys or (col[0], 'S') in update_id_keys
            if match_merge_version and match_update_id:
                filtered.append(col)
                valid_tombstone.append(col)
            else:
                # print 'Invalid tombstone ' + str(col)
                # print 'version_keys', version_keys
                # print 'update_id_keys', update_id_keys

                invalid_tombstone.append(col)
        else:
            filtered.append(col)
    return filtered, invalid_tombstone, valid_tombstone


def __sum(valid_cols, log_prefix=''):
    total = 0
    s = ''
    for item in valid_cols:
        value = item[3]
        if item[1] == 'T':
            s += ' - ' + str(value)
            total -= value
        else:
            s += ' + ' + str(value)
            total += value
    print '{0}: {1} ={2}'.format(log_prefix, total, s)
    return total


def __cols_to_str(cols):
    out = str(cols)
    out = out.replace('None', '')
    return out


def print_cols(snapshot, cols, log_prefix=''):
    if snapshot:
        print('{0}: mem: {1}'.format(log_prefix, __cols_to_str(snapshot)))
    if cols:
        print('{0}: cas: {1}'.format(log_prefix, __cols_to_str(cols)))


version = 0


def get_new_version():
    '''
    Use a integer for easy to read, in the real implementation, it should be timeuuid
    :return:
    '''
    global version
    version += 1
    return version


update_id = 0


def get_current_timeuuid(cols):
    '''
    Use a integer for easy to read, in the real implementation, it should be timeuuid
    :param cols:
    :return:
    '''
    global update_id
    last = cols[-1][0]
    update_id = max(last, update_id) + 1
    return update_id
