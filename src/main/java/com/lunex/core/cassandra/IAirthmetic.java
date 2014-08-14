package com.lunex.core.cassandra;

import java.math.BigDecimal;

public interface IAirthmetic {

	void merge(String cf, Object key, String mergedColumn);

	BigDecimal sum(String cf, Object key, String sumColumn);

	void incre(String cf, Object key, String column, BigDecimal amount);

	void commit();

	void rollback();
	
	void close();
}
