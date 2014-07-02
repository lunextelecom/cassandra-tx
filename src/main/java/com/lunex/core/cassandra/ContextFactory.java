package com.lunex.core.cassandra;

/**
 * 
 * Handle creation, getting Context.  Connection to Cassandra Cluster should be 
 * set or created here.  For now, to be simple, will only support Datastax Cassandra driver
 * 
 * @author jerryj
 *
 */
public class ContextFactory {
	public static ContextFactory getInstance(){
		throw new UnsupportedOperationException();
	}

	public IContext start(){
		throw new UnsupportedOperationException();
	}
	
	public IContext start(String contextId){
		throw new UnsupportedOperationException();
	}
	
	public IContext getContext(String contextId){
		throw new UnsupportedOperationException();
	}
	
}
