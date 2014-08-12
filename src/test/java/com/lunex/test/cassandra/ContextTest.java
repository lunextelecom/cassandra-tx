package com.lunex.test.cassandra;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.lunex.core.cassandra.Context;
import com.lunex.core.cassandra.ContextFactory;
import com.lunex.core.utils.Configuration;



/**
 * Unit test for simple App.
 */
public class ContextTest {
	private static String node = "192.168.93.38";
	private static int port = 9160;
	Cluster cluster;
	Session session;
    @BeforeClass
    public static void oneTimeSetUp() {
        // one-time initialization code   
    	System.out.println("@BeforeClass - oneTimeSetUp");
    	initEnviroment();
    	Configuration.loadConfig(node, port,"test_keyspace","tx_keyspace");
    }
 
    @AfterClass
    public static void oneTimeTearDown() {
        // one-time cleanup code
    	System.out.println("@AfterClass - oneTimeTearDown");
    	
    }
 
    @Before
    public void setUp() {
    	System.out.println("@Before - setUp");
    	
		Builder builder = Cluster.builder();
        builder.addContactPoint(node);//.withPort(Configuration.getPort());

        PoolingOptions options = new PoolingOptions();
        options.setCoreConnectionsPerHost(HostDistance.LOCAL, options.getMaxConnectionsPerHost(HostDistance.LOCAL));
        builder.withPoolingOptions(options);
        
		cluster = builder
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                .build();

		session = cluster.connect();
    }
 
    @After
    public void tearDown() {
        System.out.println("@After - tearDown");
        cluster.close();
//        discardData();
        
    }
 
    private static void initEnviroment() {
    	Builder builder = Cluster.builder();
        builder.addContactPoint(node);//.withPort(Configuration.getPort());

        PoolingOptions options = new PoolingOptions();
        options.setCoreConnectionsPerHost(HostDistance.LOCAL, options.getMaxConnectionsPerHost(HostDistance.LOCAL));
        builder.withPoolingOptions(options);
        
        Cluster cluster = builder
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                .build();

        Session session = cluster.connect();
		
    	Metadata metadata = cluster.getMetadata();
    	String keyspace = "test_keyspace";
    	String txKeyspace = "tx_keyspace";
    	KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspace);
    	KeyspaceMetadata txKeyspaceMetadata = metadata.getKeyspace(txKeyspace);
    	if(keyspaceMetadata == null){
    		String sql = "CREATE KEYSPACE test_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }";
	    	session.execute(sql);
	    	metadata = cluster.getMetadata();
	    	keyspaceMetadata = metadata.getKeyspace(keyspace);
		}
    	if(txKeyspaceMetadata == null){
    		String sql = "CREATE KEYSPACE tx_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }";
	    	session.execute(sql);
		}
    	if(metadata != null){
			keyspaceMetadata = metadata.getKeyspace(keyspace);
			if(keyspaceMetadata == null){
				throw new UnsupportedOperationException("Can't find keyspace :" + keyspace);
			}
			if(keyspaceMetadata.getTable("customer") == null){
				String sql = "CREATE TABLE test_keyspace.customer (username varchar, firstName varchar, lastName varchar, address varchar, age int, PRIMARY KEY (username))";
		    	session.execute(sql);
			}
			if(keyspaceMetadata.getTable("seller_balance") == null){
				String sql = "CREATE TABLE test_keyspace.seller_balance (id int,updateid timeuuid,type text, version text, amount decimal,PRIMARY KEY (id, updateid, type, version )) WITH CLUSTERING ORDER BY (updateid DESC)";
		    	session.execute(sql);
			}
			if(keyspaceMetadata.getTable("seller_balance_complex") == null){
				String sql = "CREATE TABLE test_keyspace.seller_balance_complex (company text, id int,updateid timeuuid,type text, version text, amount decimal,PRIMARY KEY ((company,id), updateid, type, version )) WITH CLUSTERING ORDER BY (updateid DESC)";
		    	session.execute(sql);
			}
    	}
    	cluster.close();
	}
    
    private static void discardData() {
    	Builder builder = Cluster.builder();
        builder.addContactPoint(node);//.withPort(Configuration.getPort());

        PoolingOptions options = new PoolingOptions();
        options.setCoreConnectionsPerHost(HostDistance.LOCAL, options.getMaxConnectionsPerHost(HostDistance.LOCAL));
        builder.withPoolingOptions(options);
        
        Cluster cluster = builder
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                .build();

        Session session = cluster.connect();
    	String sql = "truncate test_keyspace.customer";
    	session.execute(sql);
    	sql = "truncate test_keyspace.seller_balance";
    	session.execute(sql);
    	sql = "truncate test_keyspace.seller_balance_complex";
    	session.execute(sql);
    	
    	cluster.close();
	}
    /**
     * 
     */
    @Test
    public void testCustomer() {
    	
    	String sql = "insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)";
		session.execute(sql, "duynguyen", "Duy", "Nguyen", 20);
		
    	Context ctx = (Context) Context.start();
    	//update
    	sql = "update test_keyspace.customer set age = 26 where username = ?";
    	ctx.execute(sql, "duynguyen");
    	sql = "select * from test_keyspace.customer where username = ?";
    	ResultSet res1 = session.execute(sql, "duynguyen");
    	int age1 = res1.one().getInt("age");
    	int age2 = ctx.execute(sql, "duynguyen").get(0).getInt("age");
    	assertEquals(age1, 20);
    	assertEquals(age2, 26);
    	//rollback
    	ctx.rollback();
    	age2 = ctx.execute(sql, "duynguyen").get(0).getInt("age");
    	assertEquals(age2, 20);
    	sql = "update test_keyspace.customer set age = 26 where username = ?";
    	ctx.execute(sql, "duynguyen");
    	//commit
    	ctx.commit();
    	sql = "select * from test_keyspace.customer where username = ?";
    	res1 = session.execute(sql, "duynguyen");
    	age1 = res1.one().getInt("age");
    	assertEquals(age1, 26);
    	
    	//insert
    	sql = "insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)";
    	ctx.execute(sql, "trinhtran", "Trinh", "Tran", 20);
    	sql = "select * from test_keyspace.customer where username = ?";
    	res1 = session.execute(sql, "trinhtran");
    	assertEquals(res1.isExhausted(),true);
    	//commit
    	ctx.commit();
    	res1 = session.execute(sql, "trinhtran");
    	assertEquals(res1.isExhausted(),false);
    	
    	//delete
    	sql = "delete from test_keyspace.customer where username = ?";
    	ctx.execute(sql, "trinhtran");
    	sql = "select * from test_keyspace.customer where username = ?";
    	res1 = session.execute(sql, "trinhtran");
    	assertEquals(res1.isExhausted(),false);
    	//commit
    	ctx.commit();
    	sql = "select * from test_keyspace.customer where username = ?";
    	res1 = session.execute(sql, "trinhtran");
    	assertEquals(res1.isExhausted(),true);
    	
    	ctx.close();
	}
    
    @Test
    public void testSellerBalance() {
    	Context ctx = (Context) Context.start();
    	int id = 123;
    	//increase
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(3));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(5));
    	ctx.commit();
    	//sum
    	BigDecimal sum = ctx.sum("seller_balance", id, "amount");
    	assertEquals(sum, new BigDecimal(9));
    	//merge
    	ctx.merge("seller_balance", id, "amount");
    	//increase
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	//sum 
    	sum = ctx.sum("seller_balance", id, "amount");
    	assertEquals(sum, new BigDecimal(10));
    	ctx.rollback();
    	//sum
    	sum = ctx.sum("seller_balance", id, "amount");
    	assertEquals(sum, new BigDecimal(9));
    	//incre
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.commit();
     	//sum
    	sum = ctx.sum("seller_balance", id, "amount");
    	assertEquals(sum, new BigDecimal(10));
    	//merge
    	ctx.merge("seller_balance", id, "amount");
     	
     	//merge
     	ctx.merge("seller_balance", id, "amount");
     	
     	//decrease
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(-5));
    	ctx.commit();
    	sum = ctx.sum("seller_balance", id, "amount");
    	assertEquals(sum, new BigDecimal(5));
    	ctx.close();
	}
    
    @Test
    public void testSellerBalanceComplex() {
    	Context ctx = (Context) Context.start();
    	String table = "seller_balance_complex";
    	Map<String, Object> mapKey = new HashMap<String, Object>();
    	String company = "lunex";
    	int id = 123;
    	mapKey.put("company", company);
    	mapKey.put("id", id);
    	//increase
    	ctx.incre(table, mapKey, "amount", new BigDecimal(1));
    	ctx.incre(table, mapKey, "amount", new BigDecimal(3));
    	ctx.incre(table, mapKey, "amount", new BigDecimal(5));
    	ctx.commit();
    	//sum
    	BigDecimal sum = ctx.sum(table, mapKey, "amount");
    	assertEquals(sum, new BigDecimal(9));
    	//merge
    	ctx.merge(table, mapKey, "amount");
    	//increase
    	ctx.incre(table, mapKey, "amount", new BigDecimal(1));
    	//sum 
    	sum = ctx.sum(table, mapKey, "amount");
    	assertEquals(sum, new BigDecimal(10));
    	ctx.rollback();
    	//sum
    	sum = ctx.sum(table, mapKey, "amount");
    	assertEquals(sum, new BigDecimal(9));
    	//incre
    	ctx.incre(table, mapKey, "amount", new BigDecimal(1));
    	ctx.commit();
     	//sum
    	sum = ctx.sum(table, mapKey, "amount");
    	assertEquals(sum, new BigDecimal(10));
    	//merge
    	ctx.merge(table, mapKey, "amount");
     	
     	//merge
     	ctx.merge(table, mapKey, "amount");
     	ctx.close();
	}
    
    @Test
    public void testSelect(){
    	String sql = "insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)";
		session.execute(sql, "duynguyen", "Duy", "Nguyen", 20);
		
    	Context ctx = (Context) Context.start();
    	sql = "select * from test_keyspace.customer where username = ?";
    	assertEquals(1,ctx.execute(sql, "duynguyen").size());
    
    	ctx.close();
    }
    
    @Test
    public void testUpdate(){
    	String sql = "insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)";
		session.execute(sql, "duynguyen", "Duy", "Nguyen", 20);
		
    	Context ctx = (Context) Context.start();
    	sql = "update test_keyspace.customer set age = 26 where username = ?";
    	ctx.execute(sql, "duynguyen");
    	sql = "select * from test_keyspace.customer where username = ?";
    	ResultSet res1 = session.execute(sql, "duynguyen");
    	int age1 = res1.one().getInt("age");
    	int age2 = ctx.execute(sql, "duynguyen").get(0).getInt("age");
    	assertEquals(age1, 20);
    	assertEquals(age2, 26);
    	
    	ctx.close();
    }
    
    @Test
    public void testDelete(){
    	String sql = "insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)";
		session.execute(sql, "duynguyen", "Duy", "Nguyen", 20);
    	Context ctx = (Context) Context.start();
    	sql = "delete from test_keyspace.customer where username = ?";
    	ctx.execute(sql, "duynguyen");
    	sql = "select * from test_keyspace.customer where username = ?";
    	List<Row> res1 = ctx.execute(sql, "trinhtran");
    	assertEquals(res1.size()==0,true);
    	ctx.close();
    }
    
    @Test
    public void testInsert(){
    	String sql =  "insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)";
    	Context ctx = (Context) Context.start();
    	ctx.execute(sql, "trinhtran", "Trinh", "Tran", 20);
    	sql = "select * from test_keyspace.customer where username = ?";
    	List<Row> res1 = ctx.execute(sql, "trinhtran");
    	assertEquals(res1.size()>0,true);
    	ctx.close();
    }
    
    @Test
    public void testCommit(){
    	String sql =  "insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)";
    	Context ctx = (Context) Context.start();
    	ctx.execute(sql, "trinhtran", "Trinh", "Tran", 20);
    	ctx.commit();
    	sql = "select * from test_keyspace.customer where username = ?";
    	ResultSet res1 = session.execute(sql, "trinhtran");
    	assertEquals(res1.isExhausted(),false);
    }
    
    @Test
    public void testRollback(){
    	String sql =  "insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)";
    	Context ctx = (Context) Context.start();
    	ctx.execute(sql, "trinhtran", "Trinh", "Tran", 20);
    	ctx.rollback();
    	sql = "select * from test_keyspace.customer where username = ?";
    	ResultSet res1 = session.execute(sql, "trinhtran");
    	assertEquals(res1.isExhausted(),true);
    }
    
    @Test
    public void testIncre(){
    	Context ctx = (Context) Context.start();
    	int id = 123;
    	//increase
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(3));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(5));
    	ctx.commit();
    	String sql = "select * from test_keyspace.seller_balance where id = " + id;
    	assertEquals(3,ctx.execute(sql).size());
    }
  
    @Test
    public void testSum(){
    	Context ctx = (Context) Context.start();
    	int id = 123;
    	//increase
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(3));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(5));
    	ctx.commit();
    	String sql = "select * from test_keyspace.seller_balance where id = " + id;
    	assertEquals(new BigDecimal(9),ctx.sum("seller_balance", id, "amount"));
    }
    
    @Test
    public void testSum1(){
    	Context ctx = (Context) Context.start();
    	int id = 123;
    	String sql = "select * from test_keyspace.seller_balance where id = " + id;
    	System.out.println(ctx.sum("seller_balance", id, "amount"));
    }
    
    @Test
    public void testMerge1(){
    	Context ctx = (Context) Context.start();
    	int id = 123;
    	//increase
    	ctx.merge("seller_balance", id, "amount");
//    	ctx.merge("seller_balance", id, "amount");
    }
    
    @Test
    public void testMerge(){
    	
    	Context ctx = (Context) Context.start();
    	int id = 123;
    	//increase
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(5));
    	ctx.commit();
    	ctx.merge("seller_balance", id, "amount");
    	String sql = "select count(1) as count from test_keyspace.seller_balance where id =?";
     	assertEquals(ctx.sum("seller_balance", id, "amount") , new BigDecimal(6));
     	ctx.merge("seller_balance", id, "amount");
     	assertEquals(ctx.sum("seller_balance", id, "amount") , new BigDecimal(6));
     	ctx.incre("seller_balance", id, "amount", new BigDecimal(4));
     	ctx.commit();
     	assertEquals(ctx.sum("seller_balance", id, "amount") , new BigDecimal(10));
     	ctx.merge("seller_balance", id, "amount");
     	assertEquals(ctx.sum("seller_balance", id, "amount") , new BigDecimal(10));
     	ctx.merge("seller_balance", id, "amount");
     	assertEquals(ctx.sum("seller_balance", id, "amount") , new BigDecimal(10));
//     	ctx.merge("seller_balance", id, "amount");
//     	assertEquals(ctx.sum("seller_balance", id, "amount") , new BigDecimal(16));
//     	assertEquals(session.execute(sql,id).one().getLong("count") ,6l);
//     	
//     	assertEquals(new BigDecimal(4),ctx.sum("seller_balance", id, "amount"));
//     	ctx.merge("seller_balance", id, "amount");
//     	assertEquals(session.execute(sql,id).one().getLong("count") ,3l);
//     	
//    	assertEquals(new BigDecimal(4),ctx.sum("seller_balance", id, "amount"));
    }
   
    @Test
    public void testMergeConcurency(){
    	
    	Context ctx = (Context) Context.start();
    	int id = 123;
    	//increase
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(3));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(5));
    	ctx.commit();
    	//
		ExecutorService executor = Executors.newFixedThreadPool(50);
		for (int i = 0; i < 3; i++) {
			Context ctx1 = (Context) Context.start();
			Runnable worker = new MergeThread(ctx1, "" + i);
			executor.execute(worker);
			
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		System.out.println("Finished all threads");
    	//
		
		assertEquals(ctx.sum("seller_balance", id, "amount"),new BigDecimal(9));
    }
    
    @Test
    public void testInsert1(){
    	discardData();
    	Context ctx = (Context) Context.start();
    	Context ctx2 = (Context) Context.start();
    	int id = 123;
    	BigDecimal res = new BigDecimal(3);
    	ctx2.incre("seller_balance", id, "amount", new BigDecimal(1));
    	try {
    		Thread.sleep(2000);
    	} catch (InterruptedException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    	//increase
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.commit();
    	//
    	ctx.merge("seller_balance", id, "amount");
    	ctx2.commit();
		assertEquals(ctx.sum("seller_balance", id, "amount"),new BigDecimal(4));
    }
    
    @Test
    public void testMergeInsertConcurency(){
    	discardData();
    	Context ctx = (Context) Context.start();
    	int id = 123;
    	BigDecimal res = new BigDecimal(3);
    	//increase
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.commit();
    	//
		ExecutorService executor = Executors.newFixedThreadPool(50);
		for (int i = 0; i < 100; i++) {
			Context ctx1 = (Context) Context.start();
			if(i%2==0){
				if(i%10==0){try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}}

				Runnable worker = new MergeThread(ctx1, "" + i);
				executor.execute(worker);
			}else{
				if(i%3==0){try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}}
				res = res.add(new BigDecimal(1));
				Runnable worker = new InsertThread(ctx1, "" + i);
				executor.execute(worker);
			}
			
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		System.out.println("Finished all threads");
    	//
		
		assertEquals(ctx.sum("seller_balance", id, "amount"),res);
    }
    
    @Test
    public void testSumInsertConcurency(){
    	discardData();
    	Context ctx = (Context) Context.start();
    	int id = 123;
    	BigDecimal res = new BigDecimal(3);
    	//increase
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.commit();
    	//
		ExecutorService executor = Executors.newFixedThreadPool(50);
		for (int i = 0; i < 500; i++) {
			Context ctx1 = (Context) Context.start();
			res = res.add(new BigDecimal(1));
			Runnable worker = new InsertThread(ctx1, "" + i);
			executor.execute(worker);
			
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		System.out.println("Finished all threads");
    	//
		
		assertEquals(ctx.sum("seller_balance", id, "amount"),res);
    }
    
    //
}
