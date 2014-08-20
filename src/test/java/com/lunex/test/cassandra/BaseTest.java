package com.lunex.test.cassandra;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.lunex.core.utils.Configuration;


public class BaseTest {
	private static String node = "localhost";
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
        discardData();
        
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
}
