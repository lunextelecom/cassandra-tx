package com.lunex.core.cassandra;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Strings;
import com.lunex.core.utils.Configuration;
import com.lunex.core.utils.StatementVO;

public class Context implements IContext {

	private String ctxId;

	private ContextFactory client;
	
	//set of tx column family has changed 
	private Set<String> setCfTxChanged = new HashSet<String>();

	public void commit() {
		if(setCfTxChanged != null && !setCfTxChanged.isEmpty()){
			StringBuilder query = new StringBuilder();
			for (String cftx : setCfTxChanged) {
				query = new StringBuilder();
				query.append("SELECT * from " + cftx + " WHERE cstx_id_ = ?");
				final ResultSet results = client.getSession().execute(query.toString(), UUID.fromString(ctxId));
				if(results != null){
					while(!results.isExhausted()){
						final Row row = results.one();
						Boolean isDeleted = false;
						try{
							isDeleted = row.getBool(("cstx_deleted_"));
						}catch(Exception ex){
							isDeleted = false;
						}
						if(isDeleted){
							commitDeleteStatement(cftx, row);
						}else{
							commitUpdateStatement(cftx, row);
						}
					}
				}
			}
			//discard all record involved ctxId 
			rollback();
		}
		
	}

	private void commitUpdateStatement(String cftx, final Row row) {
		StringBuilder statement;
		StringBuilder valueSql;
		String originalCF = cftx.substring(0, cftx.length()-(Configuration.CHECKSUM_LENGTH+1));
		TableMetadata def = ContextFactory.getMapTableMetadata().get(originalCF);
		if(def != null){
			statement = new StringBuilder();
			statement.append("insert into " + originalCF + "(");
			
			valueSql = new StringBuilder(" values(");
			
			Boolean isFirst = true;
			List<Object> params = new ArrayList<Object>();
			for (ColumnMetadata child : def.getColumns()) {
				String colType = child.getType().getName().toString();
				String colName = child.getName();
				if(isFirst){
					isFirst = false;
					statement.append(colName);
					valueSql.append("?");
				}else{
					statement.append("," + colName);
					valueSql.append(",?");
				}
				addParams(row, params, colType, colName);
			}
			statement.append(")");
			valueSql.append(")");
			statement.append(valueSql);
			try {
				client.getSession().execute(statement.toString(), params.toArray());
			} catch (Exception e) {
				throw new UnsupportedOperationException("commit failed, statement : " + statement.toString());
			}
		}
	}

	private void commitDeleteStatement(String cftx, final Row row) {
		StringBuilder statement;
		String originalCF = cftx.substring(0, cftx.length()-(Configuration.CHECKSUM_LENGTH+1));
		statement = new StringBuilder();
		statement.append("delete from " + originalCF);
		Boolean isFirst = true;
		TableMetadata def = ContextFactory.getMapTableMetadata().get(originalCF);
		List<Object> params = new ArrayList<Object>();
		for (ColumnMetadata colKey : def.getPrimaryKey()) {
			String colType = colKey.getType().getName().toString();
			String colName = colKey.getName();
			if(isFirst){
				isFirst = false;
				statement.append(" where " + colName + " = ?");
			}else{
				statement.append("," + colName + " = ?");
			}
			addParams(row, params, colType, colName);
		}
		try {
			client.getSession().execute(statement.toString(), params.toArray());
		} catch (Exception e) {
			throw new UnsupportedOperationException("commit failed, statement : " + statement.toString());
		}
	}

	private void addParams(final Row row, List<Object> params, String colType,
			String colName) {
		if(colType.equals(DataType.ascii().getName().toString())
				|| colType.equals(DataType.text().getName().toString())
				|| colType.equals(DataType.varchar().getName().toString())
				){
			params.add(row.getString(colName));
		}else if(colType.equals(DataType.uuid().getName().toString()) || colType.equals(DataType.timeuuid().getName().toString())){
			params.add(row.getUUID(colName));
		}else if(colType.equals(DataType.timestamp().getName().toString())){
			params.add(row.getDate(colName));
		}else if(colType.equals(DataType.cboolean().getName().toString())){
			params.add(row.getBool(colName));
		}else if(colType.equals(DataType.blob().getName().toString())){
			params.add(row.getBytes(colName));
		}else if(colType.equals(DataType.cdouble().getName().toString())){
			params.add(row.getDouble(colName));
		}else if(colType.equals(DataType.cfloat().getName().toString())){
			params.add(row.getFloat(colName));
		}else if(colType.equals(DataType.cint().getName().toString())){
			params.add(row.getInt(colName));
		}else if(colType.equals(DataType.decimal().getName().toString())){
			params.add(row.getDecimal(colName));
		}else if(colType.equals(DataType.inet().getName().toString())){
			params.add(row.getInet(colName));
		}else if(colType.equals(DataType.counter().getName().toString()) || colType.equals(DataType.bigint().getName().toString())){
			params.add(row.getLong(colName));
		}
	}
	
	public void rollback() {
		if(setCfTxChanged != null && !setCfTxChanged.isEmpty()){
			for (String cf : setCfTxChanged) {
				try {
					client.getSession().execute("delete from " + cf + " where cstx_id_ = ?", UUID.fromString(ctxId) );
				} catch (Exception e) {
					throw new UnsupportedOperationException("rollback failed");
				}
			}
			setCfTxChanged = new HashSet<String>();
		}
		
	}

	public void merge(String cf) {
		
	}
	
	public static IContext start() {
		return ContextFactory.start();
	}
	
	public static IContext start(String contextId) {
		return ContextFactory.start(contextId);
	}
	
	public static IContext getContext(String contextId) {
		return ContextFactory.start(contextId);
	}

	public ResultSet execute(String sql, Object... args) {
		//1. parse sql to find:
		//- cf
		//- select, update, insert, delete type
		//- where condition
		ResultSet resultSet = null;
		try {
			StringBuilder sqlTX = new StringBuilder();
			StatementVO stmVO = null;
			//"update test_keyspace.customer set firstname ='quang duy' where username='duynq'"
			//1. check delete in customer_tx
			//
			//->"update test_keyspace.customer_tx set firstname ='quang duy' where cstx_id_ = uuid and username='duynq'"
			
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			Statement stm = parserManager.parse(new StringReader(sql));
			if (stm instanceof Select) {
				String tableName = ((PlainSelect)((Select) stm).getSelectBody()).getFromItem().toString();
				tableName = tableName.replaceFirst(Configuration.getKeyspace()+".", "");
				System.out.println(tableName);
				setCfTxChanged.add(ContextFactory.getMapOrgTX().get(tableName));

			} else if (stm instanceof Update) {
				Update detailStm =  (Update) stm;
				//1. generate select statement
				//2. insert into cf tx
				//3. update ct tx
				Table t = detailStm.getTable();
				sqlTX.append("update " + ContextFactory.getMapOrgTX().get(t.getName()) + " set " );
				System.out.println(((Update) stm).getTable());
				System.out.println(((Update) stm).getTable().getName());

			} else if (stm instanceof Delete) {
				String tableName = ((Delete) stm).getTable().getName();
				setCfTxChanged.add(ContextFactory.getMapOrgTX().get(tableName));
				executeDeleteStatement(ctxId, sql, (Delete) stm, args);
				System.out.println("delete");

			} else if (stm instanceof Insert) {
				String tableName = ((Insert) stm).getTable().getName();
				setCfTxChanged.add(ContextFactory.getMapOrgTX().get(tableName));
				executeInsertStatement(ctxId, (Insert) stm, args);
			}
		} catch (Exception e) {
			throw new UnsupportedOperationException();
		}
		return resultSet;
		
	}
	
	private ResultSet executesSelectStatement(String contextId, String sql, String tableName, Select info, Object... args){
		String txName = ContextFactory.getMapOrgTX().get(tableName);
		sql = sql.toLowerCase();
		sql = sql.replaceFirst(tableName, txName);
		final ResultSet results = client.getSession().execute(sql, args);
		return results;
	}
	
	private void executeDeleteStatement(String contextId, String sql, Delete info, Object... args){
		sql = sql.toLowerCase();
		sql = sql.replaceFirst("delete ", "select * ");
		final ResultSet results = client.getSession().execute(sql, args);
		if(results != null){
			String txName = ContextFactory.getMapOrgTX().get(info.getTable().getName());
			String wholeName = info.getTable().getSchemaName() + "." + txName;
			TableMetadata def = ContextFactory.getMapTableMetadata().get(info.getTable().getName());
			List<Object> params = new ArrayList<Object>();
			StringBuilder insertSql = new StringBuilder();
			StringBuilder valueSql = new StringBuilder();
			if(txName != null && def != null){
				while(!results.isExhausted()){
					final Row row = results.one();
					params = new ArrayList<Object>();
					insertSql = new StringBuilder();
					valueSql = new StringBuilder();
					insertSql.append("insert into " + wholeName + "(cstx_id_, cstx_updateid_, cstx_deleted_");
					valueSql.append(" values(?,now(),true");
					params.add(UUID.fromString(contextId));
					for (ColumnMetadata col : def.getColumns()) {
						insertSql.append("," + col.getName());
						valueSql.append(",?");
						addParams(row, params, col.getType().getName().toString(), col.getName());
					}
					insertSql.append(")");
					valueSql.append(")");
					insertSql.append(valueSql);
					client.getSession().execute(insertSql.toString(), params.toArray());
				}
			}
		}
	}
	
	private void executeInsertStatement(String contextId, Insert info, Object... args){
		StringBuilder insertSql = new StringBuilder();
		List<Object> params = new ArrayList<Object>();
		insertSql.append("insert into " + info.getTable().getSchemaName() + "." + ContextFactory.getMapOrgTX().get(info.getTable().getName())+ "(cstx_id_, cstx_updateid_");
		for (Object obj : info.getColumns()) {
			insertSql.append("," + ((Column)obj).getColumnName());
		}
		insertSql.append(") values(?,now()");
		params.add(UUID.fromString(contextId) );
		List<Object> listValue =  ((ExpressionList) info.getItemsList()).getExpressions();
		int i = 0;
		for (Object obj : listValue) {
			if(obj instanceof JdbcParameter){
				params.add(args[i]);
				i++;
			}else{
				if(obj instanceof String){
					String val = obj.toString();
					params.add(val);
				}else{
					params.add(obj.toString());
				}
			}
			insertSql.append(",?");
		}
		insertSql.append(")");
		client.getSession().execute(insertSql.toString(), params.toArray());
	}
	//get,set
	public String getCtxId() {
		return ctxId;
	}
	
	public void setCtxId(String ctxId) {
		this.ctxId = ctxId;
	}

	public ContextFactory getClient() {
		return client;
	}

	public void setClient(ContextFactory client) {
		this.client = client;
	}

}

