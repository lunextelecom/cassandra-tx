package com.lunex.core.cassandra;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.lunex.core.utils.Configuration;
import com.lunex.core.utils.RowKey;
import com.lunex.core.utils.Utils;

// TODO: Auto-generated Javadoc
/**
 * The Class Context.
 */
public class Context implements IContext {

	/** The Constant logger. */
	private static final Logger logger = LoggerFactory.getLogger(Context.class);

	/** The ctx id. */
	private String ctxId;

	/** The client. */
	private ContextFactory client;

	/**
	 * Start.
	 *
	 * @return the IContext
	 */
	public static IContext start() {
		return ContextFactory.start();
	}

	/**
	 * Start.
	 *
	 * @param contextId
	 * @return the Context
	 */
	public static IContext start(String contextId) {
		return ContextFactory.start(contextId);
	}

	/**
	 * Gets the context.
	 *
	 * @param contextId the context id
	 * @return the context
	 */
	public static IContext getContext(String contextId) {
		return ContextFactory.getContext(contextId);
	}

	/* (non-Javadoc)
	 * @see com.lunex.core.cassandra.IContext#commit()
	 */
	public void commit() {
		logger.info("context commit");
		Set<String> setCfTxChanged = getTablesChange();
		if (setCfTxChanged != null && !setCfTxChanged.isEmpty()) {
			StringBuilder query = new StringBuilder();
			BatchStatement batch = new BatchStatement();
			BoundStatement bs = null;
			for (String cftx : setCfTxChanged) {
				query = new StringBuilder();
				// get data from tmp table
				query.append("SELECT * from " + Utils.getFullTXCF(cftx)
						+ " WHERE cstx_id_ = ?");
				final ResultSet results = executeNonContext(query.toString(),
						UUID.fromString(ctxId));
				logger.debug(query.toString());
				if (results != null) {
					while (!results.isExhausted()) {
						final Row row = results.one();
						// check whether row is deleted
						Boolean isDeleted = false;
						try {
							isDeleted = row.getBool(("cstx_deleted_"));
						} catch (Exception ex) {
							isDeleted = false;
						}
						if (isDeleted) {
							// commit delete statement
							bs = commitDeleteStatement(cftx, row);
							if (bs != null) {
								batch.add(bs);
							}
						} else {
							// commit others statement
							bs = commitOthersStatement(cftx, row);
							if (bs != null) {
								batch.add(bs);
							}
						}
						executeBatch(batch, false);
					}
				}
			}
			executeBatch(batch, true);
			// discard all record involved ctxId
			rollback();
		}

	}

	/**
	 * Commit others statement.
	 *
	 * @param cftx : temporary table 
	 * @param row 
	 * @return the bound statement
	 */
	private BoundStatement commitOthersStatement(String cftx, final Row row) {
		StringBuilder statement;
		StringBuilder valueSql;
		// get original table from tmp table: customer_91ec1f93 -> customer
		String originalCF = cftx.substring(0, cftx.length()
				- (Configuration.CHECKSUM_LENGTH + 1));
		Boolean isArith = row.getBool("is_arith_");
		// get tablemetadata from dictionary
		TableMetadata def = ContextFactory.getMapTableMetadata()
				.get(originalCF);
		if (def != null) {
			// generation insert statement for original table
			statement = new StringBuilder();
			statement.append("insert into "
					+ Utils.getFullOriginalCF(originalCF) + "(");

			valueSql = new StringBuilder(" values(");

			Boolean isFirst = true;
			List<Object> params = new ArrayList<Object>();
			for (ColumnMetadata child : def.getColumns()) {
				String colType = child.getType().getName().toString();
				String colName = child.getName();
				if (isFirst) {
					isFirst = false;
					statement.append(colName);
					if (isArith && colName.equalsIgnoreCase("updateid")) {
						valueSql.append("now()");
					} else {
						valueSql.append("?");
						params.add(RowKey.getValue(row, colType, colName));
					}
				} else {
					statement.append("," + colName);
					if (isArith && colName.equalsIgnoreCase("updateid")) {
						valueSql.append(",now()");
					} else {
						valueSql.append(",?");
						params.add(RowKey.getValue(row, colType, colName));
					}
				}
			}
			statement.append(")");
			valueSql.append(")");
			statement.append(valueSql);
			try {
				BoundStatement ps = prepareStatement(statement.toString(),
						params);
				logger.debug(statement.toString());
				return ps;
			} catch (Exception e) {
				logger.error("commit failed, statement : "
						+ statement.toString() + ". Message :" + e.getMessage());
				throw new UnsupportedOperationException(
						"commit failed, statement : " + statement.toString()
								+ ". Message :" + e.getMessage());
			}
		}
		return null;
	}

	/**
	 * Commit delete statement.
	 *
	 * @param cftx : temporary table
	 * @param row 
	 * @return the bound statement
	 */
	private BoundStatement commitDeleteStatement(String cftx, final Row row) {
		StringBuilder statement;
		// get original table from tmp table: customer_91ec1f93 -> customer
		String originalCF = cftx.substring(0, cftx.length()
				- (Configuration.CHECKSUM_LENGTH + 1));
		// generate delete statement
		statement = new StringBuilder();
		statement.append("delete from " + Utils.getFullOriginalCF(originalCF));
		Boolean isFirst = true;
		TableMetadata def = ContextFactory.getMapTableMetadata()
				.get(originalCF);
		List<Object> params = new ArrayList<Object>();
		for (ColumnMetadata colKey : def.getPrimaryKey()) {
			// init primary keys in where condition
			String colType = colKey.getType().getName().toString();
			String colName = colKey.getName();
			if (isFirst) {
				isFirst = false;
				statement.append(" where " + colName + " = ?");
			} else {
				statement.append(" and " + colName + " = ?");
			}
			params.add(RowKey.getValue(row, colType, colName));

		}
		try {
			BoundStatement ps = prepareStatement(statement.toString(), params);
			logger.debug(statement.toString());
			return ps;
		} catch (Exception e) {
			logger.error("commit failed, statement : " + statement.toString()
					+ ". Message :" + e.getMessage());
			throw new UnsupportedOperationException(
					"commit failed, statement : " + statement.toString()
							+ ". Message :" + e.getMessage());
		}
	}

	/**
	 * Discard context.
	 *
	 * @param isClosed = True: delete contextId in database 
	 */
	private void discardContext(Boolean isClosed) {
		// rollback all changes, delete tmp table has cstx_id_ = ctxId
		logger.info("context rollback");
		Set<String> setCfTxChanged = getTablesChange();
		BatchStatement batch = new BatchStatement();
		BoundStatement bs = null;
		List<Object> params;
		String statement = "";
		if (setCfTxChanged != null && !setCfTxChanged.isEmpty()) {
			for (String cf : setCfTxChanged) {
				try {
					params = new ArrayList<Object>();
					params.add(UUID.fromString(ctxId));
					statement = "delete from " + Utils.getFullTXCF(cf) + " where cstx_id_ = ?"; 
					bs = prepareStatement(statement, params);
					logger.debug(statement);
					if (bs != null) {
						batch.add(bs);
					}
					executeBatch(batch, false);
				} catch (Exception e) {
					throw new UnsupportedOperationException("rollback failed");
				}
			}
			if (isClosed) {
				params = new ArrayList<Object>();
				params.add(UUID.fromString(ctxId));
				statement = "delete from " + Utils.getFullTXCF("cstx_context") + " where contextid = ?";
				bs = prepareStatement(statement, params);
				logger.debug(statement);
				if (bs != null) {
					batch.add(bs);
				}
			}
			executeBatch(batch, true);
		}
	}

	/* (non-Javadoc)
	 * @see com.lunex.core.cassandra.IContext#rollback()
	 */
	public void rollback() {
		discardContext(false);
	}

	/* (non-Javadoc)
	 * @see com.lunex.core.cassandra.IContext#close()
	 */
	public void close() {
		discardContext(true);
	}

	/* (non-Javadoc)
	 * @see com.lunex.core.cassandra.IContext#execute(java.lang.String, java.lang.Object[])
	 */
	public List<Row> execute(String sql, Object... args) {
		return execute(sql, false, args);

	}

	/* (non-Javadoc)
	 * @see com.lunex.core.cassandra.IContext#execute4Arithmetic(java.lang.String, java.lang.Object[])
	 */
	public void execute4Arithmetic(String sql, Object... args) {
		execute(sql, true, args);

	}

	/* (non-Javadoc)
	 * @see com.lunex.core.cassandra.IContext#executeNonContext(java.lang.String, java.lang.Object[])
	 */
	public ResultSet executeNonContext(String sql, Object... arguments) {
		return client.getSession().execute(sql, arguments);
	}

	/**
	 * execute.
	 *
	 * @param sql the sql
	 * @param isArith : execute arithmetic record
	 * @param args 
	 * @return the list< row>
	 */
	private List<Row> execute(String sql, Boolean isArith,
			Object... args) {
		logger.info("context executing");
		List<Row> res = null;
		try {
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			Statement stm = parserManager.parse(new StringReader(sql));
			Set<String> setCfTxChanged = getTablesChange();
			if (stm instanceof Select) {
				// select statement
				sql = sql.toLowerCase();
				String tableName = ((PlainSelect) ((Select) stm)
						.getSelectBody()).getFromItem().toString()
						.toLowerCase();
				int index = tableName.indexOf(Configuration.getKeyspace()
						.toLowerCase() + ".");
				if (index == -1) {
					// input doesnot contain keyspace
					sql = sql.replaceFirst(tableName,
							Utils.getFullOriginalCF(tableName));
				} else {
					tableName = tableName.replaceFirst(Configuration
							.getKeyspace().toLowerCase() + ".", "");
				}
				res = executesSelectStatement(ctxId, sql, tableName, isArith,
						args);

			} else if (stm instanceof Update) {
				// update statement
				String tableName = ((Update) stm).getTable().getName();
				updateTablesChange(setCfTxChanged, ContextFactory.getMapOrgTX()
						.get(tableName));
				executeUpdateStatement(sql, (Update) stm, isArith, args);

			} else if (stm instanceof Delete) {
				// delete statement
				String tableName = ((Delete) stm).getTable().getName();
				updateTablesChange(setCfTxChanged, ContextFactory.getMapOrgTX()
						.get(tableName));
				executeDeleteStatement(ctxId, sql, (Delete) stm, args);

			} else if (stm instanceof Insert) {
				// insert statement
				String tableName = ((Insert) stm).getTable().getName();
				updateTablesChange(setCfTxChanged, ContextFactory.getMapOrgTX()
						.get(tableName));
				executeInsertStatement(ctxId, (Insert) stm, isArith, args);
			}
		} catch (Exception e) {
			logger.error("context executed failed" + ". Message :"
					+ e.getMessage());
			throw new UnsupportedOperationException(". Message :"
					+ e.getMessage());
		}
		return res;

	}

	/**
	 * Execute update statement.
	 *
	 * @param sql 
	 * @param info 
	 * @param isArith
	 * @param args 
	 */
	private void executeUpdateStatement(String sql, Update info,
			Boolean isArith, Object... args) {
		logger.info("execute update statement");
		/*
		 * 1. generate select statement 2. insert into tmp table 3. execute
		 * update statement on tmp table
		 */
		String orgName = info.getTable().getName();
		String txName = ContextFactory.getMapOrgTX().get(orgName);
		// 1. generate select statement
		Boolean isReplace = false;
		String tmpString = "_fixedJSqlParserBug_";
		for (Object obj : info.getExpressions()) {
			if (obj instanceof Function) {
				if (((Function) obj).getParameters() == null) {
					ExpressionList expr = new ExpressionList();
					List<Object> listparam = new ArrayList<Object>();
					listparam.add(tmpString);
					expr.setExpressions(listparam);
					((Function) obj).setParameters(expr);
					isReplace = true;
				}

			}
		}
		String selectSql = null;

		if (isReplace) {
			selectSql = info.toString().replaceAll(tmpString, "").toLowerCase();
		} else {
			selectSql = info.toString().toLowerCase();
		}
		selectSql = selectSql.replaceFirst("update ", "select * from ");
		List<Row> lstRow = executesSelectStatement(ctxId, selectSql, orgName,
				isArith, args);
		logger.debug(selectSql);
		// 2. insert into tmp table
		BatchStatement batch = new BatchStatement();
		BoundStatement bs = null;
		if (lstRow != null && !lstRow.isEmpty()) {
			List<Object> params = new ArrayList<Object>();
			StringBuilder insertSql = new StringBuilder();
			StringBuilder valueSql = new StringBuilder();
			TableMetadata def = ContextFactory.getMapTableMetadata().get(
					orgName);
			for (Row row : lstRow) {

				params = new ArrayList<Object>();
				insertSql = new StringBuilder();
				valueSql = new StringBuilder();
				insertSql.append("insert into " + Utils.getFullTXCF(txName)
						+ "(cstx_id_,is_arith_");
				valueSql.append(" values(?,?");
				params.add(UUID.fromString(ctxId));
				params.add(isArith);
				for (ColumnMetadata col : def.getColumns()) {
					insertSql.append("," + col.getName());
					valueSql.append(",?");
					params.add(RowKey.getValue(row, col.getType().getName()
							.toString(), col.getName()));
				}
				insertSql.append(")");
				valueSql.append(")");
				insertSql.append(valueSql);
				bs = prepareStatement(insertSql.toString(), params);
				logger.debug(insertSql.toString());
				if (bs != null) {
					batch.add(bs);
				}
				executeBatch(batch, false);
			}
		}

		// 3. execute update statement on tmp table
		StringBuilder updateSql = new StringBuilder(sql.replaceFirst(info
				.getTable().getWholeTableName(), Utils.getFullTXCF(txName))
				+ " and cstx_id_ = ?");
		List<Object> params = new ArrayList<Object>();
		for (int i = 0; i < args.length; i++) {
			params.add(args[i]);
		}
		params.add(UUID.fromString(ctxId));
		bs = prepareStatement(updateSql.toString(), params);
		logger.debug(updateSql.toString());
		if (bs != null) {
			batch.add(bs);
		}
		executeBatch(batch, true);
	}

	/**
	 * Executes select statement.
	 *
	 * @param contextId the context id
	 * @param sql the sql
	 * @param tableName the table name
	 * @param isArith the is arith
	 * @param args the args
	 * @return the list< row>
	 */
	private List<Row> executesSelectStatement(String contextId, String sql,
			String tableName, Boolean isArith, Object... args) {
		logger.info("execute select statement");

		/*
		 * 1. get data from original table 2. get data from tmp table 3. return
		 * data from tmp table if exists, otherwise return data from original
		 * table
		 */

		// 1. get data from original table
		ResultSet results = executeNonContext(sql, args);
		List<Row> lstOrg = new ArrayList<Row>();
		if (results != null) {
			while (!results.isExhausted()) {
				final Row row = results.one();
				lstOrg.add(row);
				if (isArith) {
					if (row.getString("version").contains("Head")) {
						break;
					}
				}
			}
		}
		// 2. get data from tmp table
		TableMetadata def = ContextFactory.getMapTableMetadata().get(tableName);
		String txName = ContextFactory.getMapOrgTX().get(tableName);
		StringBuilder txSql = new StringBuilder(sql.toString().replaceFirst(
				Utils.getFullOriginalCF(tableName), Utils.getFullTXCF(txName)));

		txSql.insert(txSql.indexOf("where", txSql.indexOf(txName)) + 5,
				" cstx_id_ = ? and ");
		List<Object> params = new ArrayList<Object>();
		params.add(UUID.fromString(contextId));
		for (int i = 0; i < args.length; i++) {
			params.add(args[i]);
		}
		results = executeNonContext(txSql.toString(), params.toArray());
		logger.debug(txSql.toString());
		Map<RowKey, Row> mapRow = new HashMap<RowKey, Row>();
		if (results != null) {
			while (!results.isExhausted()) {
				Row row = results.one();
				RowKey tmp = new RowKey();
				tmp.setRow(row);
				tmp.setKeys(def.getPrimaryKey());
				mapRow.put(tmp, row);
			}
		}
		// 3. return data from tmp table if exists, otherwise return data from
		// original table
		List<Row> res = new ArrayList<Row>();
		for (Row row : mapRow.values()) {
			res.add(row);
		}
		if (lstOrg != null && lstOrg.size() > 0) {
			for (Row child : lstOrg) {
				RowKey tmp = new RowKey();
				tmp.setRow(child);
				tmp.setKeys(def.getPrimaryKey());
				if (mapRow.containsKey(tmp)) {
					res.add(mapRow.get(tmp));
				} else {
					res.add(child);
				}
			}
		}
		return res;
	}

	/**
	 * Execute delete statement.
	 *
	 * @param contextId the context id
	 * @param sql the sql
	 * @param info the info
	 * @param args the args
	 */
	private void executeDeleteStatement(String contextId, String sql,
			Delete info, Object... args) {

		logger.info("execute delete statement");

		/*
		 * 1. select data from original table 2. insert into tmp table with
		 * cstx_deleted_ = true
		 */

		// 1. select data from original table
		sql = sql.toLowerCase();
		sql = sql.replaceFirst("delete ", "select * ");
		final ResultSet results = executeNonContext(sql, args);

		// 2. insert into tmp table with cstx_deleted_ = true
		BatchStatement batch = new BatchStatement();
		BoundStatement bs = null;
		if (results != null) {
			String txName = ContextFactory.getMapOrgTX().get(
					info.getTable().getName());

			TableMetadata def = ContextFactory.getMapTableMetadata().get(
					info.getTable().getName());
			List<Object> params = new ArrayList<Object>();
			StringBuilder insertSql = new StringBuilder();
			StringBuilder valueSql = new StringBuilder();
			if (txName != null && def != null) {
				while (!results.isExhausted()) {
					final Row row = results.one();
					params = new ArrayList<Object>();
					insertSql = new StringBuilder();
					valueSql = new StringBuilder();
					insertSql.append("insert into " + Utils.getFullTXCF(txName)
							+ "(cstx_id_, cstx_deleted_");
					valueSql.append(" values(?, true");
					params.add(UUID.fromString(contextId));
					for (ColumnMetadata col : def.getPrimaryKey()) {
						insertSql.append("," + col.getName());
						valueSql.append(",?");
						params.add(RowKey.getValue(row, col.getType().getName()
								.toString(), col.getName()));
					}
					insertSql.append(")");
					valueSql.append(")");
					insertSql.append(valueSql);
					bs = prepareStatement(insertSql.toString(), params);
					logger.debug(insertSql.toString());
					if (bs != null) {
						batch.add(bs);
					}
					executeBatch(batch, false);
				}
			}
			executeBatch(batch, true);
		}
	}

	/**
	 * Execute insert statement.
	 *
	 * @param contextId the context id
	 * @param info the info
	 * @param isArith the is arith
	 * @param args the args
	 */
	private void executeInsertStatement(String contextId, Insert info,
			Boolean isArith, Object... args) {
		logger.info("execute insert statement");

		/*
		 * generate insert statement from tmp table
		 */
		String orgName = info.getTable().getName();
		String txName = ContextFactory.getMapOrgTX().get(orgName);
		info.getTable().setName(txName);
		info.getTable().setSchemaName(Configuration.getTxKeyspace());
		info.getColumns().add("cstx_id_");
		info.getColumns().add("is_arith_");

		List<Object> params = new ArrayList<Object>();
		for (int i = 0; i < args.length; i++) {
			params.add(args[i]);
		}
		params.add(UUID.fromString(ctxId));
		params.add(isArith);
		Boolean isReplace = false;
		String tmpString = "_fixedJSqlParserBug_";
		for (Object obj : ((ExpressionList) info.getItemsList())
				.getExpressions()) {
			if (obj instanceof Function) {
				if (((Function) obj).getParameters() == null) {
					ExpressionList expr = new ExpressionList();
					List<Object> listparam = new ArrayList<Object>();
					listparam.add(tmpString);
					expr.setExpressions(listparam);
					((Function) obj).setParameters(expr);
					isReplace = true;
				}

			}
		}

		StringBuilder insertSql = null;

		if (isReplace) {
			insertSql = new StringBuilder(info.toString().replaceAll(tmpString,
					""));
		} else {
			insertSql = new StringBuilder(info.toString());
		}
		insertSql = insertSql.replace(insertSql.lastIndexOf(")"),
				insertSql.length(), ",?,?)");
		executeNonContext(insertSql.toString(), params.toArray());
		logger.debug(insertSql.toString());
	}

	/**
	 * Update tables change.
	 *
	 * @param input the input
	 * @param tableName the table name
	 */
	private void updateTablesChange(Set<String> input, String tableName) {
		if (!input.contains(tableName)) {
			Set<String> lstTable = new HashSet<String>();
			for (String child : input) {
				lstTable.add(child);
			}
			lstTable.add(tableName);
			StringBuilder sql = new StringBuilder("update "
					+ Utils.getFullTXCF("cstx_context")
					+ " set lstcfname = ?, updateid=now() where contextid = ?");
			try {
				List<Object> params = new ArrayList<Object>();
				params.add(lstTable);
				params.add(UUID.fromString(ctxId));
				executeNonContext(sql.toString(), params.toArray());
				logger.debug(sql.toString());
			} catch (Exception e) {
				logger.error("updateTablesChange failed :" + sql
						+ ". Message :" + e.getMessage());
				throw new UnsupportedOperationException(
						"updateTablesChange failed :" + sql + ". Message :"
								+ e.getMessage());
			}
		}
	}

	/**
	 * Gets the tables change.
	 *
	 * @return the tables change
	 */
	private Set<String> getTablesChange() {
		Set<String> res = new HashSet<String>();
		StringBuilder sql = new StringBuilder("select * from "
				+ Utils.getFullTXCF("cstx_context") + " where contextid = ?");
		try {
			List<Object> params = new ArrayList<Object>();
			params.add(UUID.fromString(ctxId));
			ResultSet resultSet = executeNonContext(sql.toString(),
					params.toArray());
			logger.debug(sql.toString());
			if (resultSet != null && !resultSet.isExhausted()) {
				final Row row = resultSet.one();
				res = row.getSet("lstcfname", String.class);
			}
		} catch (Exception e) {
			logger.error("getTablesChange failed :" + sql + ". Message :"
					+ e.getMessage());
			throw new UnsupportedOperationException("getTablesChange failed :"
					+ sql + ". Message :" + e.getMessage());
		}
		return res;
	}

	/* (non-Javadoc)
	 * @see com.lunex.core.cassandra.IContext#prepareStatement(java.lang.String, java.util.List)
	 */
	public BoundStatement prepareStatement(String sql, List<Object> params) {
		if (params == null) {
			params = new ArrayList<Object>();
		}

		BoundStatement ps = client.getSession().prepare(sql)
				.bind(params.toArray());

		return ps;
	}

	/* (non-Javadoc)
	 * @see com.lunex.core.cassandra.IContext#executeBatch(com.datastax.driver.core.BatchStatement, java.lang.Boolean)
	 */
	public void executeBatch(BatchStatement batch, Boolean forceRun) {
		if (forceRun) {
			if (batch.getStatements() != null
					&& batch.getStatements().size() > 0) {
				client.getSession().execute(batch);
				batch.clear();
			}
		} else {
			if (batch.getStatements() != null
					&& batch.getStatements().size()
							% Configuration.getBatchSize() == 0) {
				client.getSession().execute(batch);
				batch.clear();
			}
		}
	}

	
	/**
	 * Gets the ctx id.
	 *
	 * @return the ctx id
	 */
	public String getCtxId() {
		return ctxId;
	}

	/**
	 * Sets the ctx id.
	 *
	 * @param ctxId the ctx id
	 */
	public void setCtxId(String ctxId) {
		this.ctxId = ctxId;
	}

	/**
	 * Gets the client.
	 *
	 * @return the client
	 */
	public ContextFactory getClient() {
		return client;
	}

	/**
	 * Sets the client.
	 *
	 * @param client the client
	 */
	public void setClient(ContextFactory client) {
		this.client = client;
	}

}
