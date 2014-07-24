package com.lunex.core.cassandra;

import java.math.BigDecimal;
import java.util.List;

import com.datastax.driver.core.Row;

public interface IContext {

	List<Row> execute(String sql, Object... arguments);
	void commit();
	void rollback();
	void merge(String cf, Object key, String mergedColumn);
	BigDecimal sum(String cf, Object key, String sumColumn);
	void incre(String cf, Object key, String column, BigDecimal amount);
}
