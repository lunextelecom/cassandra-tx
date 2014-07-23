package com.lunex.core.cassandra;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.lunex.core.utils.Configuration;



/**
 * Unit test for simple App.
 */
public class ContextTest {
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
    	
    }
 
    @After
    public void tearDown() {
        System.out.println("@After - tearDown");
        
    }
 
    private static void initEnviroment() {
    	Cluster cluster = Cluster.builder().addContactPoint("localhost").withPort(9042).build();
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
				String sql = "CREATE TABLE test_keyspace.seller_balance (company text,id bigint,updateid timeuuid,amount decimal,ismerged boolean,PRIMARY KEY((company,id), updateid))";
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
    	cluster.close();
	}

    //test case here
    @Test
    public void testInsert() {
		String sql = "insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)";
    	System.out.println(sql);
    	Context ctx = (Context) Context.start();
    	ctx.execute(sql, "duynguyen", "Duy", "Nguyen", 26);
    	//select
    	sql = "select * from test_keyspace.customer where username=?";
    	List<Row> rows = ctx.execute(sql, "duynguyen");
    	System.out.println(sql);
    	assertEquals(1, rows.size());
    	ctx.commit();
	}
    
    //
}
