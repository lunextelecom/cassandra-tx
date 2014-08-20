package com.lunex.test.cassandra;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.lunex.core.cassandra.Arithmetic;
import com.lunex.core.cassandra.Context;


public class TransactionTest extends BaseTest{
   
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
    
    //
}
