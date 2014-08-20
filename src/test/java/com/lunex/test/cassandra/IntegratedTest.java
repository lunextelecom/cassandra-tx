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

public class IntegratedTest extends BaseTest{
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
    	Arithmetic atm = new Arithmetic(true);
    	int id = 123;
    	//increase
    	atm.incre("seller_balance", id, "amount", new BigDecimal(1));
    	atm.incre("seller_balance", id, "amount", new BigDecimal(3));
    	atm.incre("seller_balance", id, "amount", new BigDecimal(5));
    	atm.commit();
    	//sum
    	BigDecimal sum = atm.sum("seller_balance", id, "amount");
    	assertEquals(sum, new BigDecimal(9));
    	//merge
    	atm.merge("seller_balance", id, "amount");
    	//increase
    	atm.incre("seller_balance", id, "amount", new BigDecimal(1));
    	//sum 
    	sum = atm.sum("seller_balance", id, "amount");
    	assertEquals(sum, new BigDecimal(10));
    	atm.rollback();
    	//sum
    	sum = atm.sum("seller_balance", id, "amount");
    	assertEquals(sum, new BigDecimal(9));
    	//incre
    	atm.incre("seller_balance", id, "amount", new BigDecimal(1));
    	atm.commit();
     	//sum
    	sum = atm.sum("seller_balance", id, "amount");
    	assertEquals(sum, new BigDecimal(10));
    	//merge
    	atm.merge("seller_balance", id, "amount");
     	
     	//merge
     	atm.merge("seller_balance", id, "amount");
     	
     	//decrease
    	atm.incre("seller_balance", id, "amount", new BigDecimal(-5));
    	atm.commit();
    	sum = atm.sum("seller_balance", id, "amount");
    	assertEquals(sum, new BigDecimal(5));
    	atm.close();
	}
    
    @Test
    public void testSellerBalanceComplex() {
    	Arithmetic atm = new Arithmetic(true);
    	String table = "seller_balance_complex";
    	Map<String, Object> mapKey = new HashMap<String, Object>();
    	String company = "lunex";
    	int id = 123;
    	mapKey.put("company", company);
    	mapKey.put("id", id);
    	//increase
    	atm.incre(table, mapKey, "amount", new BigDecimal(1));
    	atm.incre(table, mapKey, "amount", new BigDecimal(3));
    	atm.incre(table, mapKey, "amount", new BigDecimal(5));
    	atm.commit();
    	//sum
    	BigDecimal sum = atm.sum(table, mapKey, "amount");
    	assertEquals(sum, new BigDecimal(9));
    	//merge
    	atm.merge(table, mapKey, "amount");
    	//increase
    	atm.incre(table, mapKey, "amount", new BigDecimal(1));
    	//sum 
    	sum = atm.sum(table, mapKey, "amount");
    	assertEquals(sum, new BigDecimal(10));
    	atm.rollback();
    	//sum
    	sum = atm.sum(table, mapKey, "amount");
    	assertEquals(sum, new BigDecimal(9));
    	//incre
    	atm.incre(table, mapKey, "amount", new BigDecimal(1));
    	atm.commit();
     	//sum
    	sum = atm.sum(table, mapKey, "amount");
    	assertEquals(sum, new BigDecimal(10));
    	//merge
    	atm.merge(table, mapKey, "amount");
     	
     	//merge
     	atm.merge(table, mapKey, "amount");
     	atm.close();
	}
    
}
