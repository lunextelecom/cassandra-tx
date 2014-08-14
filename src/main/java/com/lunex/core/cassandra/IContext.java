package com.lunex.core.cassandra;

import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public interface IContext {
	List<Row> execute(String sql, Object... arguments);

	void execute4Airthmetic(String sql, Object... arguments);
	
	void commit();

	void rollback();

	//close context
	void close();

	ResultSet executeNonContext(String sql, Object... arguments);
	
	BoundStatement prepareStatement(String sql, List<Object> params);
	
	void executeBatch(BatchStatement batch, Boolean forceRun);
}
