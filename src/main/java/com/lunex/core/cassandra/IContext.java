package com.lunex.core.cassandra;

import com.datastax.driver.core.ResultSet;

public interface IContext {

	ResultSet execute(String sql, Object... arguments);
	void commit();
	void rollback();
	void merge(String cf);
}
