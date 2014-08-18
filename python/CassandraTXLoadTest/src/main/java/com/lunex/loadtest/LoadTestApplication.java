package com.lunex.loadtest;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.lunex.core.utils.Configuration;

public class LoadTestApplication extends Application<LoadTestConfiguration> {
    public static String node = "192.168.93.38";

    public static void main(String[] args) throws Exception {
	initEnviroment();
	Configuration.loadConfig(node, 9042, "test_keyspace", "tx_keyspace");
	String[] tmp = "server,example.yml".split(",");
	new LoadTestApplication().run(tmp);
    }

    private static void initEnviroment() {
	Builder builder = Cluster.builder();
	builder.addContactPoint(node);// .withPort(Configuration.getPort());

	PoolingOptions options = new PoolingOptions();
	options.setCoreConnectionsPerHost(HostDistance.LOCAL,
		options.getMaxConnectionsPerHost(HostDistance.LOCAL));
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
	if (keyspaceMetadata == null) {
	    String sql = "CREATE KEYSPACE test_keyspace WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 2 }";
	    session.execute(sql);
	    metadata = cluster.getMetadata();
	    keyspaceMetadata = metadata.getKeyspace(keyspace);
	}
	if (txKeyspaceMetadata == null) {
	    String sql = "CREATE KEYSPACE tx_keyspace WITH REPLICATION =  { 'class' : 'NetworkTopologyStrategy', 'dc1' : 2 }";
	    session.execute(sql);
	}
	if (metadata != null) {
	    keyspaceMetadata = metadata.getKeyspace(keyspace);
	    if (keyspaceMetadata == null) {
		throw new UnsupportedOperationException("Can't find keyspace :"
			+ keyspace);
	    }
	    if (keyspaceMetadata.getTable("customer") == null) {
		String sql = "CREATE TABLE test_keyspace.customer (username varchar, firstName varchar, lastName varchar, address varchar, age int, PRIMARY KEY (username))";
		session.execute(sql);
	    }
	    if (keyspaceMetadata.getTable("seller_balance") == null) {
		String sql = "CREATE TABLE test_keyspace.seller_balance (id int,updateid timeuuid,type text, version text, amount decimal,PRIMARY KEY (id, updateid, type, version )) WITH CLUSTERING ORDER BY (updateid DESC)";
		session.execute(sql);
	    }
	    if (keyspaceMetadata.getTable("customer_balance") == null) {
		String sql = "CREATE TABLE test_keyspace.customer_balance (id int,updateid timeuuid,type text, version text, amount decimal,PRIMARY KEY (id, updateid, type, version )) WITH CLUSTERING ORDER BY (updateid DESC)";
		session.execute(sql);
	    }
	}
	cluster.close();
    }

    @Override
    public String getName() {
	return "hello-world";
    }

    @Override
    public void initialize(Bootstrap<LoadTestConfiguration> bootstrap) {
	// nothing to do yet
    }

    @Override
    public void run(LoadTestConfiguration configuration, Environment environment) {
	final LoadTestService resource = new LoadTestService(
		configuration.getTemplate(), configuration.getDefaultName());
	environment.jersey().register(resource);
    }
}
