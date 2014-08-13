Performance Test
================

Standone console program using the library directly.  This will isolate other factor such as webframework, serialization although they should be minimal compared to database IO.

##CRUD testing
Test mix crud operation.
```
Load Parameter: 
	Num of client: Number of thread/process sending request	
	Num of unique Object for update, delete: Want those client to update the same object to test for concurrency.
	Num of Request: total number of request send.	
		create:
		update:
		delete:
		list:
	Num of Commit: Find out the impact if now all order are commit, but rather some are rolledback.
	Num of Context:	if this number is same as Request, then all number use context, otherwise, some will not use context.
	Tables for the test: the loadtest can create the table.

function:
	create
	update
	delete
	list
	
```
##Arithmetic functions

The ratio customer to seller will determine contention.  Customer will random send to the available seller.  At the end of the loadtest or at any given moment, the total of balance of all seller, customer should be the same.  Both purchase or transfer does not create any money.

```
Load Parameter: 
	Num of client: Number of thread/process executing fucntion
	Num of unique customer: Want those client to update the same object to test for concurrency.
	Num of unique seller:
	Amount of $ in customer: default 1,000,000
	Amount of $ in seller: default 0
	
	Num of request:
		transfer: total number of transfer
		purchase: total number of purchase
	Num of Commit: Find out the impact if now all order are commit, but rather some are rolledback.
	Num of Context:	if this number is same as Request, then all number use context, otherwise, some will not use context.

	Tables for the test: loadtest should create table seller, customer

function:
	purchase(customerid, sellerid, amount) #move $ from customer to seller.
	transfer(sellerid, sellerid, amount) # move $ from seller to another seller
```
