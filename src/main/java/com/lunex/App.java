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
	
	private static void testDelete() {
		String sql = "delete from test_keyspace.customer where username = ?";
    	Context ctx = (Context) Context.start();
    	ctx.execute(sql, "hungnm");
    	ctx.commit();
	}
	
}
