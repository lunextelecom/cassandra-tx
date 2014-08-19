package com.lunex.test.cassandra;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import com.lunex.core.cassandra.Arithmetic;
import com.lunex.core.cassandra.Context;


public class ArithmeticTest extends ContextTest {

	@Test
    public void testIncre(){
    	Arithmetic atm = new Arithmetic(true);
    	int id = 123;
    	//increase
    	atm.incre("seller_balance", id, "amount", new BigDecimal(1));
    	atm.incre("seller_balance", id, "amount", new BigDecimal(3));
    	atm.incre("seller_balance", id, "amount", new BigDecimal(5));
    	atm.commit();
    	Context ctx = (Context) Context.start();
    	String sql = "select * from test_keyspace.seller_balance where id = " + id;
    	assertEquals(3,ctx.execute(sql).size());
    }
  
    @Test
    public void testSum(){
    	Arithmetic atm = new Arithmetic(true);
    	int id = 123;
    	//increase
    	atm.incre("seller_balance", id, "amount", new BigDecimal(1));
    	atm.incre("seller_balance", id, "amount", new BigDecimal(3));
    	atm.incre("seller_balance", id, "amount", new BigDecimal(5));
    	atm.commit();
    	assertEquals(new BigDecimal(9),atm.sum("seller_balance", id, "amount"));
    }
    
    @Test
    public void testMerge(){
    	
    	Arithmetic ctx = new Arithmetic(true);
    	int id = 123;
    	//increase
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(5));
    	ctx.commit();
    	ctx.merge("seller_balance", id, "amount");
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
    }
   
    @Test
    public void testMergeConcurency(){
    	Arithmetic atm = new Arithmetic(true);
    	int id = 123;
    	//increase
    	atm.incre("seller_balance", id, "amount", new BigDecimal(1));
    	atm.incre("seller_balance", id, "amount", new BigDecimal(3));
    	atm.incre("seller_balance", id, "amount", new BigDecimal(5));
    	atm.commit();
    	//
		ExecutorService executor = Executors.newFixedThreadPool(50);
		for (int i = 0; i < 3; i++) {
			Arithmetic ctx1 = new Arithmetic(true);
			Runnable worker = new MergeThread(ctx1, "" + i);
			executor.execute(worker);
			
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		System.out.println("Finished all threads");
    	//
		
		assertEquals(atm.sum("seller_balance", id, "amount"),new BigDecimal(9));
    }
    
    @Test
    public void testInsertMerge(){
    	Arithmetic atm = new Arithmetic(true);
    	Arithmetic atm2 = new Arithmetic(true);
    	int id = 123;
    	atm2.incre("seller_balance", id, "amount", new BigDecimal(1));
    	//increase
    	atm.incre("seller_balance", id, "amount", new BigDecimal(1));
    	atm.incre("seller_balance", id, "amount", new BigDecimal(1));
    	atm.incre("seller_balance", id, "amount", new BigDecimal(1));
    	atm.commit();
    	//
    	atm.merge("seller_balance", id, "amount");
    	atm2.commit();
		assertEquals(atm.sum("seller_balance", id, "amount"),new BigDecimal(4));
    }
    
    @Test
    public void testMergeInsertConcurency(){
    	Arithmetic ctx = new Arithmetic(true);
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
			Arithmetic ctx1 = new Arithmetic(true);
			if(i%2==0){
				Runnable worker = new MergeThread(ctx1, "" + i);
				executor.execute(worker);
			}else{
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
    	Arithmetic ctx = new Arithmetic(true);
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
			Arithmetic ctx1 = new Arithmetic(true);
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

    @Test
    public void testInsertNoTransaction(){
    	Arithmetic ctx = new Arithmetic(false);
    	int id = 123;
    	ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
		assertEquals(ctx.sum("seller_balance", id, "amount"),new BigDecimal(1));
		ctx.close();
    }
    
    //
}
