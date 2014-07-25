Algorithm for Arithmetic
===================
Merge is when a rows of number in cassandra need to be converted to a sum and converted to a single number.
```
[1] --Incre 3 -->[1,3]
[1,3] -->Incre 5 --> [1,3,5]
[1,3,5] --Sum--> 9
[1,3,5] --Merge--> [9]
```
Possible case to what during the insertion of 9, and removal of the old record, the reader must be have extra logics to avoid double counting(when 9 is inserted, but pre merge data is not remove yet) or missed count pre-merge data is removed and 9 is not inserted.

Add a version string value to all record.
Reader should always read from left to right.  That is reverse order in cassandra and it is very easy to do.  We should define the table to be as such
```
CREATE TABLE seller_balance (    
    id int,   
    updateid timeuuid,
    version text,
    amount decimal,
    PRIMARY KEY (id, updateid )
) WITH CLUSTERING ORDER BY (updateid DESC);

insert 1
insert 3
insert 5
insert 6 (by another thread), but this end up finishing after merge)
merge
insert 7 (by another thread)
issert 8

[(1,1),(1,3),(1,5)] 
[(1,1),(1,3),(2Head,9)] #the last record ws replaced since the the same updateid was used.  Record left of 2 is consider dead record.  Also decided to make it 2Head as a convention to tell it is the first of the new record of version 2.
[(1,1),(1,3),(2Head,9),(1,6)] #somehow another thread inserted with old version
[(1,1),(1,3),(2Head,9),(1,6),(2,7) # another record is inserted, this time version 2 since the last record was version 2, (1,6) didnt' get inserted yet


```
* incre, decre: appends new record. Always determine the version from the latest updateid record.  In the above case, it is determine previous version is 1, so 2 is used.  Do not worry if a merge might insert a higher version.

* sum (read), read from latest updateid to oldest until either a Head record is found or all records are read.  Sum this value.

* merge, use sum to determine the sum.  copy down the newest updateid record.  Insert the sum with that upateid which will overwrite with version = max(version of records) + 1.  Records less than updateid can be deleted.  Even if it failed, it is not a big deal as the sum already know how to ignore these records, and the next merge will clean it up.
