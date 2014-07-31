'''
Created on Jul 30, 2014

@author: DuyNguyen
'''
import datetime
import uuid
import time
from cassandra.cluster import Cluster

# get offset in seconds between the UUID timestamp Epoch (1582-10-15)and
# the Epoch used on this computer
DTD_SECS_DELTA = (datetime.datetime(*time.gmtime(0)[0:3])-
datetime.datetime(1582, 10, 15)).days * 86400
def uuid1_to_ts(u):

    secs_uuid1 = u.time / 1e7
    secs_epoch = secs_uuid1 - DTD_SECS_DELTA
    return datetime.datetime.fromtimestamp(secs_epoch)

    #print uuid1_to_ts(uuid.UUID("eb230bf0-1707-11e4-8984-3d8f6b70a3a0"))
def getDiffMinute(uuid1, uuid2):
    diff = abs(uuid1_to_ts(uuid2)-uuid1_to_ts(uuid1))
    diff_minutes = diff.seconds/60
    return diff_minutes

'''cstx_context (contextid uuid, lstcfname set<text>, updateid timeuuid, PRIMARY KEY (contextid))'''
def main():
    cluster = Cluster()
    session = cluster.connect('tx_keyspace')
    
    result = session.execute("select * from  tx_keyspace.cstx_context")
    lastestUUID = None
    lstContext = []
    for row in result:
        if lastestUUID == None :
            lastestUUID = row.updateid
        elif (row.updateid.time > lastestUUID.time) :
            lastestUUID = row.updateid
        lstContext.append(row)
    for row in lstContext:
        '''delete context have updateid < lastestTimeuuid - 10(min)'''
        if(getDiffMinute(row.updateid,lastestUUID) > 10):
            try:
                lstTable = row.lstcfname
                if lstTable:
                    for table in lstTable:
                        session.execute("delete from tx_keyspace." + table + " where cstx_id_ = " + str(row.contextid) )
                session.execute("delete from tx_keyspace.cstx_context where contextid = " + str(row.contextid) )
            except :
                pass

if __name__ == "__main__":
    main()
    
