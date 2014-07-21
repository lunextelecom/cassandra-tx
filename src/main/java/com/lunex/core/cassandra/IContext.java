package com.lunex.core.cassandra;

import java.util.List;

import com.datastax.driver.core.Row;

public interface IContext {

	List<Row> execute(String sql, Object... arguments);
	void commit();
	void rollback();
	void merge(String cf);
}
