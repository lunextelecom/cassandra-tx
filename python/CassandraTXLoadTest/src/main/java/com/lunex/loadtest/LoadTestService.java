package com.lunex.loadtest;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.lunex.core.cassandra.Context;

@Path("/testtx")
@Produces(MediaType.APPLICATION_JSON)
public class LoadTestService {
    private final String template;
    private final String defaultName;
    private final AtomicLong counter;

    public LoadTestService(String template, String defaultName) {
        this.template = template;
        this.defaultName = defaultName;
        this.counter = new AtomicLong();
    }

    @Path("/resetdata")
    @GET
    @Timed
    public String reset() {
    	try {
	    	String sql = ""; 
	    	Context ctx = (Context) Context.start();
	    	sql = "truncate test_keyspace.customer_balance";
	    	ctx.executeNonContext(sql.toString());
	    	
	    	sql = "truncate test_keyspace.seller_balance";
	    	ctx.executeNonContext(sql.toString());
	
	    	sql = "truncate test_keyspace.customer";
	    	ctx.executeNonContext(sql.toString());
	    	
	    	//create 10 customer
	    	for (int i = 1; i <= 10; i++) {
	    		sql = "insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)";
		    	ctx.executeNonContext(sql.toString(), i+"", i+"", i+"", 1);
			}
	    	//
	    	//create 10 seller_balance
	    	for (int i = 1; i <= 10; i++) {
		    	ctx.incre("seller_balance", i, "amount", new BigDecimal(0));
			}
	    	//create 10 customer_balance, total amount = 1.000.000 
	    	int totalCustomer = 10;
	    	for (int i = 1; i <= totalCustomer; i++) {
		    	ctx.incre("customer_balance", i, "amount", new BigDecimal(1000000/totalCustomer));
			}
	    	ctx.commit();
	    	ctx.close();
    	} catch (Exception e) {
    		return "{error:true,message:" + e.getMessage() + "}";
    	}
    	return "{error:false}";
    }
    
    
    @Path("/mixcrud")
    @GET
    @Timed
    public String mixCrud(@QueryParam("isDelete")Boolean isDelete, @QueryParam("isCommit") Boolean isCommit, @QueryParam("isWait")Boolean isWait) {
    	try {
	    	String sql = ""; 
	    	Context ctx = (Context) Context.start();
	    	String username = UUID.randomUUID().toString();
	    	sql = "insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)";
	    	ctx.execute(sql.toString(), username, username, username, 1);
	    	
	    	sql = "update test_keyspace.customer set age = 26 where username = ?" ;
	    	ctx.execute(sql.toString(), username);
	
	    	sql= "select * from test_keyspace.customer where username = ?";
	    	ctx.execute(sql.toString(), username);
	    	
	    	if(isDelete){
		    	sql ="delete from test_keyspace.customer where username = ?";
		    	ctx.execute(sql.toString(), username);
	    	}
	    	if(isWait){
	    		Thread.sleep(2000);
	    	}
	    	ctx.commit();
	    	ctx.close();
    	} catch (Exception e) {
    		return "{error:true,message:" + e.getMessage() + "}";
    	}
    	return "{error:false}";
    }
    
    @Path("/crud")
    @GET
    @Timed
    public String testCrud(@QueryParam("username") String username, @QueryParam("crud") String crud,  @QueryParam("isCommit") Boolean isCommit,@QueryParam("isUseContext") Boolean isUseContext) {
    	StringBuilder sql = new StringBuilder(); 
    	Context ctx = (Context) Context.start();
    	if(isUseContext){
    		if(crud.equalsIgnoreCase("S")){
    			sql.append("select * from test_keyspace.customer where username = ?");
    			ctx.execute(sql.toString(), username);
    			ctx.close();
    		}else if(crud.equalsIgnoreCase("I")){
    			sql.append("insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)");
    	    	ctx.execute(sql.toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), 1);
    	    	if(isCommit){
    	    		ctx.commit();
    	    	}
    			ctx.close();
    		}else if(crud.equalsIgnoreCase("U")){
    			//update
    	    	sql.append("update test_keyspace.customer set age = 26 where username = ?");
    	    	ctx.execute(sql.toString(), username);
    	    	if(isCommit){
    	    		ctx.commit();
    	    	}
    			ctx.close();
    		}else if(crud.equalsIgnoreCase("D")){
    			//update
    			sql.append("delete from test_keyspace.customer where username = ?");
    	    	ctx.execute(sql.toString(), username);
    	    	if(isCommit){
    	    		ctx.commit();
    	    	}
    			ctx.close();
    		}
    	}else{
    		if(crud.equalsIgnoreCase("S")){
    			sql.append("select * from test_keyspace.customer where username = ?");
    			ctx.executeNonContext(sql.toString(), username);
    		}else if(crud.equalsIgnoreCase("I")){
    			sql.append("insert into test_keyspace.customer(username, firstname, lastname, age) values(?,?,?,?)");
    	    	ctx.executeNonContext(sql.toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), 1);
    		}else if(crud.equalsIgnoreCase("U")){
    			//update
    	    	sql.append("update test_keyspace.customer set age = 26 where username = ?");
    	    	ctx.executeNonContext(sql.toString(), username);
    		}else if(crud.equalsIgnoreCase("D")){
    			//update
    			sql.append("delete from test_keyspace.customer where username = ?");
    	    	ctx.executeNonContext(sql.toString(), username);
    		}
    		ctx.close();
    	}
    	return "{error:false}";
    }
    
    @Path("/arithmetic")
    @GET
    @Timed
    public String testAirthMetic(@QueryParam("id") int id, @QueryParam("type") String type,  @QueryParam("isCommit") Boolean isCommit) {
    	StringBuilder sql = new StringBuilder(); 
    	Context ctx = (Context) Context.start();
		if(type.equalsIgnoreCase("I")){
			sql.append("select * from test_keyspace.customer where username = ?");
			ctx.incre("seller_balance", id, "amount", new BigDecimal(1));
			if(isCommit){
	    		ctx.commit();
	    	}
			ctx.close();
		}else if(type.equalsIgnoreCase("S")){
			BigDecimal sum = ctx.sum("seller_balance", id, "amount");
			ctx.close();
			return "{sum:" + sum.longValue() + "}";
		}else if(type.equalsIgnoreCase("M")){
			//merge
	    	ctx.merge("seller_balance", id, "amount");
			ctx.close();
		}
    	return "{error:false}";
    }
    
    @Path("/purchase")
    @GET
    @Timed
    public String purchase(@QueryParam("customerId") int customerId, @QueryParam("sellerId") int sellerId,  @QueryParam("amount") int amount, @QueryParam("isCommit") Boolean isCommit, @QueryParam("isWait") Boolean isWait, @QueryParam("isMerge") Boolean isMerge) {
    	//move $ from customer to seller.
    	try {
	    	Context ctx = (Context) Context.start();
	    	ctx.incre("customer_balance", customerId, "amount", new BigDecimal(-amount));
	    	ctx.incre("seller_balance", sellerId, "amount", new BigDecimal(amount));
	    	if(isMerge){
        		ctx.merge("customer_balance", customerId, "amount");
        		ctx.merge("seller_balance", sellerId, "amount");
        	}
	    	if(isWait){
				Thread.sleep(2000);
	    	}
			if(isCommit){
	    		ctx.commit();
	    	}
			ctx.close();
	    	return "{error:false}";
    	} catch (Exception e) {
    		return "{error:true,message:" + e.getMessage() + "}";
    	}
    }
    
    @Path("/transfer")
    @GET
    @Timed
    public String transfer(@QueryParam("fromSellerId") int fromSellerId, @QueryParam("toSellerId") int toSellerId,  @QueryParam("amount") int amount, @QueryParam("isCommit") Boolean isCommit, @QueryParam("isWait") Boolean isWait, @QueryParam("isMerge") Boolean isMerge) {
    	//move $ from fromSellerId to toSellerId.
    	try {
    		Context ctx = (Context) Context.start();
        	ctx.incre("seller_balance", fromSellerId, "amount", new BigDecimal(-amount));
        	ctx.incre("seller_balance", toSellerId, "amount", new BigDecimal(amount));
        	if(isMerge){
        		ctx.merge("seller_balance", fromSellerId, "amount");
        		ctx.merge("seller_balance", toSellerId, "amount");
        	}
        	if(isWait){
        		Thread.sleep(2000);
        	}
        	if(isCommit){
        		ctx.commit();
        	}
    		ctx.close();
        	return "{error:false}";
    	} catch (Exception e) {
    		return "{error:true,message:" + e.getMessage() + "}";
    	}
    	
    }
    
    @Path("/sumAll")
    @GET
    @Timed
    public String sumAll(@QueryParam("cf") String cf ) {
    	try {
    		BigDecimal sum = new BigDecimal(0);
    		Context ctx = (Context) Context.start();
    		for(int i = 1; i <=10; i++){
    			sum = sum.add(ctx.sum(cf, i, "amount"));
    		}
    		ctx.close();
        	return "{sum:" +sum.longValue() + "}";
    	} catch (Exception e) {
    		return "{error:true,message:" + e.getMessage() + "}";
    	}
    	
    }
    
    @Path("/mergeAll")
    @GET
    @Timed
    public String mergeAll(@QueryParam("cf") String cf ) {
    	try {
    		Context ctx = (Context) Context.start();
    		for(int i = 1; i <=10; i++){
    			ctx.merge(cf, i, "amount");
    		}
    		ctx.close();
        	return "{error:false}";
    	} catch (Exception e) {
    		return "{error:true,message:" + e.getMessage() + "}";
    	}
    	
    }
}