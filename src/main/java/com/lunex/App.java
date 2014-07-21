package com.lunex;

import java.util.List;

import com.datastax.driver.core.Row;
import com.lunex.core.cassandra.Context;
import com.lunex.core.utils.Configuration;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ){
    	Configuration.loadConfig("localhost", 9042,"test_keyspace");
//    	testUpdate();
    	testInsert();
//    	testSelect();
    	/*String sql = "delete from test_keyspace.customer where username = ?";
    	Context ctx = (Context) Context.start();
    	ctx.execute(sql, "hungnm");
    	ctx.commit();*/
    }

	private static void testInsert() {
		String sql = "insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)";
    	System.out.println(sql);
    	Context ctx = (Context) Context.start();
    	ctx.execute(sql, "trinhtran", "Trinh", "Tran", 25);
    	//select
    	sql = "select * from test_keyspace.customer where username=?";
    	List<Row> rows = ctx.execute(sql, "trinhtran");
    	System.out.println(sql);
    	
    	ctx.commit();
	}

	private static void testSelect() {
		String sql = "select * from test_keyspace.customer where username=?";
    	System.out.println(sql);
    	Context ctx = (Context) Context.start();
    	ctx.execute(sql, "baolvt");
    	ctx.commit();
	}
	private static void testUpdate() {
		String sql = "update test_keyspace.customer set age = 26 where username=?";
    	System.out.println(sql);
    	Context ctx = (Context) Context.start();
    	ctx.execute(sql, "baolvt");
    	ctx.commit();
	}

	
	public static void main1(String[] args) {
		
//		final CassandraConnector client = new CassandraConnector();
//		final String ipAddress = args.length > 0 ? args[0] : "localhost";
//		final int port = args.length > 1 ? Integer.parseInt(args[1]) : 9042;
//		System.out.println("Connecting to IP Address " + ipAddress + ":" + port
//				+ "...");
//		client.connect(ipAddress, port);
//		List<Object> params = new ArrayList<Object>();
//		params.add("hungnm");
//		params.add(null);
//		params.add(24);
//		params.add("Hung");
//		params.add("Nguyen");
//		client.getSession().execute(
//						"insert into test_keyspace.customer(username,address,age,firstname,lastname) values(?,?,?,?,?)",
//						params.toArray());
//		final ResultSet movieResults = client.getSession().execute("describe columnfamily test_keyspace.customer");
		String n = "customer_91ec1f91"; 
		String tmpCheckSum = n.substring(n.length()-8, n.length());
		String name = n.substring(0, n.length()-9);
		System.out.println(tmpCheckSum);
		System.out.println(name);
				
		String tmp = "CREATE TABLE test_keyspace.customer (ctx_id uuid,ctx_updateid timeuuid,username text,address text,age int,ctx_deleted boolean,ctx_where_condition text,firstname text,lastname text,PRIMARY KEY (ctx_id, ctx_updateid, username)) WITH read_repair_chance = 0.0 AND dclocal_read_repair_chance = 0.1 AND replicate_on_write = true AND gc_grace_seconds = 864000 AND bloom_filter_fp_chance = 0.01 AND caching = 'KEYS_ONLY' AND comment = '' AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy' } AND compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.LZ4Compressor' } AND default_time_to_live = 0 AND speculative_retry = '99.0PERCENTILE' AND index_interval = 128;";
//		String tmp = "CREATE TABLE test_keyspace.customer (ctx_id uuid,";
		System.out.println(tmp);
		tmp = tmp.replace("CREATE TABLE test_keyspace.customer ", "CREATE TABLE test_keyspace.customer_tx ");
		
		int beginIndex = tmp.indexOf("PRIMARY KEY (");
		int endIndex = tmp.indexOf (")",beginIndex);
		String pri = tmp.substring(beginIndex, endIndex+1);
		tmp = tmp.replace(pri, "pri(1,2,3)");
		System.out.println(tmp);
		
	}
	
}
