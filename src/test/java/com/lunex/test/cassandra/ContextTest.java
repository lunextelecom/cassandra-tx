package com.lunex.test.cassandra;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.lunex.core.cassandra.Context;
import com.lunex.core.utils.Configuration;



/**
 * Unit test for simple App.
 */
public class ContextTest {
	private static String node = "localhost";
	private static int port = 9042;
	Cluster cluster;
	Session session;
    @BeforeClass
    public static void oneTimeSetUp() {
        // one-time initialization code   
    	System.out.println("@BeforeClass - oneTimeSetUp");
    	initEnviroment();
    	Configuration.loadConfig("localhost", 9042,"test_keyspace","tx_keyspace");
    }
 
    @AfterClass
    public static void oneTimeTearDown() {
        // one-time cleanup code
    	System.out.println("@AfterClass - oneTimeTearDown");
    	discardData();
    }
 
    @Before
    public void setUp() {
    	System.out.println("@Before - setUp");
    	
    	cluster = Cluster.builder().addContactPoint(node).withPort(port)
				.build();
		session = cluster.connect();
    }
 
    @After
    public void tearDown() {
        System.out.println("@After - tearDown");
        
    }
 
    private static void initEnviroment() {
    	Cluster cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
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
    	Cluster cluster = Cluster.builder().addContactPoint("localhost").withPort(9042).build();
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
    	String sql = "select count(1) as count from test_keyspace.seller_balance where id =?";
    	assertEquals(session.execute(sql,id).one().getLong("count") ,1l);
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
    	sql = "select count(1) as count from test_keyspace.seller_balance where id =?";
     	assertEquals(session.execute(sql,id).one().getLong("count") ,2l);
     	//sum
    	sum = ctx.sum("seller_balance", id, "amount");
    	assertEquals(sum, new BigDecimal(10));
    	//merge
    	ctx.merge("seller_balance", id, "amount");
    	sql = "select count(1) as count from test_keyspace.seller_balance where id =?";
     	assertEquals(session.execute(sql,id).one().getLong("count") ,1l);
     	
     	//merge
     	ctx.merge("seller_balance", id, "amount");
    	sql = "select count(1) as count from test_keyspace.seller_balance where id =?";
     	assertEquals(session.execute(sql,id).one().getLong("count") ,1l);
     	
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
    	String sql = "select count(1) as count from test_keyspace.seller_balance_complex where company = '" + company + "' and id = " + id;
    	assertEquals(session.execute(sql).one().getLong("count") ,1l);
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
    	sql = "select count(1) as count from test_keyspace.seller_balance_complex where company = '" + company + "' and id = " + id;
     	assertEquals(session.execute(sql).one().getLong("count") ,2l);
     	//sum
    	sum = ctx.sum(table, mapKey, "amount");
    	assertEquals(sum, new BigDecimal(10));
    	//merge
    	ctx.merge(table, mapKey, "amount");
    	sql = "select count(1) as count from test_keyspace.seller_balance_complex where company = '" + company + "' and id = " + id;
     	assertEquals(session.execute(sql).one().getLong("count") ,1l);
     	
     	//merge
     	ctx.merge(table, mapKey, "amount");
    	sql = "select count(1) as count from test_keyspace.seller_balance_complex where company = '" + company + "' and id = " + id;
     	assertEquals(session.execute(sql).one().getLong("count") ,1l);
     	ctx.close();
	}
    
  
    //
}
