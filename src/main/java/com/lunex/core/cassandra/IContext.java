package com.lunex.core.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.util.List;

/**
 * The Interface IContext.
 */
public interface IContext {
	
	/**
	 * Execute.
	 *
	 * @param statement 
	 * @param arguments 
	 * @return the list< row>
	 */
	List<Row> execute(String statement, Object... arguments);

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

	/**
	 * Execute non context : cassandra operation
	 *
	 * @param statement 
	 * @param arguments 
	 * @return the result set
	 */
        ResultSet executeNoTx(String statement, Object... arguments);

  /**
	 * Prepare statement.
	 *
	 * @param statement 
	 * @param params 
	 * @return the bound statement
	 */
	BoundStatement prepareStatement(String statement, List<Object> params);
	
	/**
	 * Execute batch.
	 *
	 * @param batch the batch
	 * @param forceRun = True: force execute batch
	 */
	void executeBatch(BatchStatement batch, Boolean forceRun);
}
