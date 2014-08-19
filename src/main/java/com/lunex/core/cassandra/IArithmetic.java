package com.lunex.core.cassandra;

import java.math.BigDecimal;

/**
 * The Interface IArithmetic.
 */
public interface IArithmetic {

	/**
	 * Merge.
	 *
	 * @param cf : column family
	 * @param key : key of row
	 * @param column : name of column for sum
	 */
	void merge(String cf, Object key, String column);

	/**
	 * Sum.
	 *
	 * @param cf the cf
	 * @param key the key
	 *  @param column : name of column for sum
	 * @return the big decimal
	 */
	BigDecimal sum(String cf, Object key, String column);

	/**
	 * Incre.
	 *
	 * @param cf : column family
	 * @param key : key of row
	 * @param column : name of column for incre
	 * @param amount 
	 */
	void incre(String cf, Object key, String column, BigDecimal amount);

	/**
	 * Commit.
	 */
	void commit();

	/**
	 * Rollback.
	 */
	void rollback();
	
	/**
	 * Close.
	 */
	void close();
}
