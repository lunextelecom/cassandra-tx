package com.lunex.test.cassandra;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;

import com.lunex.core.cassandra.Context;


public class ContextTest extends BaseTest {
	
	@Test
    public void start(){
		Context ctx = (Context) Context.start();
		assertEquals(ctx.getCtxId()!=null, true);
    }
	
	@Test
    public void startCustomContext(){
		//start new custom contextid
		String contextId = UUID.randomUUID().toString();
		Context ctx = (Context) Context.start(contextId);
		assertEquals(ctx.getCtxId()!=null, true);
		
		//start with existed context
		boolean thrown = false;
		try {
			Context.start(contextId);
		} catch (UnsupportedOperationException e) {
			thrown = true;
		}
		assertEquals(thrown, true);
		
		//close context
		ctx.close();
    }
	
	@Test
    public void getContext(){
		Context ctx = (Context) Context.start();
		
		//get existed context
		boolean thrown = false;
		try {
			Context.getContext(ctx.getCtxId());
		} catch (UnsupportedOperationException e) {
			thrown = true;
		}
		assertEquals(thrown, false);
		
		//get not existed context
		thrown = false;
		try {
			Context.getContext(UUID.randomUUID().toString());
		} catch (UnsupportedOperationException e) {
			thrown = true;
		}
		assertEquals(thrown, true);
		
		//close context
		ctx.close();
    }
	
}
