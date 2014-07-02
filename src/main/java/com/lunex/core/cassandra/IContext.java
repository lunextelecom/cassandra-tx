package com.lunex.core.cassandra;

public interface IContext {
	
	void commit();
	void rollback();
	void merge(String cf);
}
