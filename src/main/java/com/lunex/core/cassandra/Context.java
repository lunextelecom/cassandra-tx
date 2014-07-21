package com.lunex.core.cassandra;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.lunex.core.utils.Configuration;
import com.lunex.core.utils.RowKey;

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
							commitOthersStatement(cftx, row);
						}
					}
				}
			}
			//discard all record involved ctxId 
			rollback();
		}
		
	}

	private void commitOthersStatement(String cftx, final Row row) {
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
				params.add(RowKey.getValue(row, colType, colName));
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
			params.add(RowKey.getValue(row, colType, colName));
		}
		try {
			client.getSession().execute(statement.toString(), params.toArray());
		} catch (Exception e) {
			throw new UnsupportedOperationException("commit failed, statement : " + statement.toString());
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
		return ContextFactory.getContext(contextId);
	}

	public List<Row> execute(String sql, Object... args) {
		List<Row> res = null;
		try {
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			Statement stm = parserManager.parse(new StringReader(sql));
			if (stm instanceof Select) {
				String tableName = ((PlainSelect)((Select) stm).getSelectBody()).getFromItem().toString().toLowerCase();
				tableName = tableName.replaceFirst(Configuration.getKeyspace().toLowerCase()+".", "");
				setCfTxChanged.add(ContextFactory.getMapOrgTX().get(tableName));
				executesSelectStatement(ctxId, sql, tableName, args);

			} else if (stm instanceof Update) {
				String tableName = ((Update) stm).getTable().getName();
				setCfTxChanged.add(ContextFactory.getMapOrgTX().get(tableName));
				executeUpdateStatement(sql, (Update) stm, args);

			} else if (stm instanceof Delete) {
				String tableName = ((Delete) stm).getTable().getName();
				setCfTxChanged.add(ContextFactory.getMapOrgTX().get(tableName));
				executeDeleteStatement(ctxId, sql, (Delete) stm, args);

			} else if (stm instanceof Insert) {
				String tableName = ((Insert) stm).getTable().getName();
				setCfTxChanged.add(ContextFactory.getMapOrgTX().get(tableName));
				executeInsertStatement(ctxId, (Insert) stm, args);
			}
		} catch (Exception e) {
			throw new UnsupportedOperationException();
		}
		return res;
		
	}

	private void executeUpdateStatement(String sql, Update info, 
			Object... args) {
		String orgName = info.getTable().getName();
		String txName = ContextFactory.getMapOrgTX().get(orgName);
		String txWholeName = info.getTable().getSchemaName() + "." + txName;
		String selectSql = info.toString().toLowerCase();
		selectSql = selectSql.replaceFirst("update " + info.getTable().getWholeTableName(), "select * from " + orgName);
		List<Row> lstRow = executesSelectStatement(ctxId, selectSql, orgName, args);
		//insert into tx table
		if(lstRow != null && !lstRow.isEmpty()){
			List<Object> params = new ArrayList<Object>();
			StringBuilder insertSql = new StringBuilder();
			StringBuilder valueSql = new StringBuilder();
			TableMetadata def = ContextFactory.getMapTableMetadata().get(orgName);
			for (Row row : lstRow) {
				
				params = new ArrayList<Object>();
				insertSql = new StringBuilder();
				valueSql = new StringBuilder();
				insertSql.append("insert into " + txWholeName + "(cstx_id_");
				valueSql.append(" values(?");
				params.add(UUID.fromString(ctxId));
				for (ColumnMetadata col : def.getColumns()) {
					insertSql.append("," + col.getName());
					valueSql.append(",?");
					params.add(RowKey.getValue(row, col.getType().getName().toString(), col.getName()));
				}
				insertSql.append(")");
				valueSql.append(")");
				insertSql.append(valueSql);
				client.getSession().execute(insertSql.toString(), params.toArray());
			}
		}
			
		//
		//update tx table
		StringBuilder updateSql = new StringBuilder(sql.replaceFirst(info.getTable().getWholeTableName(), txName) + " and cstx_id_ = ?");
		List<Object> params = new ArrayList<Object>();
		for(int i = 0; i < args.length; i++){
			params.add(args[i]);
		}
		params.add(UUID.fromString(ctxId));
		client.getSession().execute(updateSql.toString(), params.toArray());
	}
	
	private List<Row> executesSelectStatement(String contextId, String sql, String tableName, Object... args){
		ResultSet results = client.getSession().execute(sql, args);
		List<Row> lstOrg = new ArrayList<Row>();
		if(results != null){
			while(!results.isExhausted()){
				lstOrg.add(results.one());
			}
		}
		
		//
		TableMetadata def = ContextFactory.getMapTableMetadata().get(tableName);
		//
		
		String txName = ContextFactory.getMapOrgTX().get(tableName);
		StringBuilder txSql = new StringBuilder(sql.toString().replaceFirst(tableName, txName)); 
		txSql.append(" and cstx_id_ = ?");
		List<Object> params = new ArrayList<Object>();
		for(int i = 0; i < args.length; i++){
			params.add(args[i]);
		}
		params.add(UUID.fromString(contextId));
		results = client.getSession().execute(txSql.toString(), params.toArray());
		Map<RowKey, Row> mapRow = new HashMap<RowKey, Row>();
		if(results != null){
			while(!results.isExhausted()){
				Row row = results.one();
				RowKey tmp = new RowKey();
				tmp.setRow(row);
				tmp.setKeys(def.getPrimaryKey());
				mapRow.put(tmp,row);
			}
		}
		List<Row> res = new ArrayList<Row>(); 
		for (Row child : lstOrg) {
			RowKey tmp = new RowKey();
			tmp.setRow(child);
			tmp.setKeys(def.getPrimaryKey());
			if(mapRow.containsKey(tmp)){
				res.add(mapRow.get(tmp));
			}else{
				res.add(child);
			}
		}
		return res;
	}
	
	private void executeDeleteStatement(String contextId, String sql, Delete info, Object... args){
		sql = sql.toLowerCase();
		sql = sql.replaceFirst("delete ", "select * ");
		final ResultSet results = client.getSession().execute(sql, args);
		if(results != null){
			String txName = ContextFactory.getMapOrgTX().get(info.getTable().getName());
			String txWholeName = info.getTable().getSchemaName() + "." + txName;
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
					insertSql.append("insert into " + txWholeName + "(cstx_id_, cstx_deleted_");
					valueSql.append(" values(?, true");
					params.add(UUID.fromString(contextId));
					for (ColumnMetadata col : def.getColumns()) {
						insertSql.append("," + col.getName());
						valueSql.append(",?");
						params.add(RowKey.getValue(row, col.getType().getName().toString(), col.getName()));
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
		insertSql.append("insert into " + info.getTable().getSchemaName() + "." + ContextFactory.getMapOrgTX().get(info.getTable().getName())+ "(cstx_id_");
		for (Object obj : info.getColumns()) {
			insertSql.append("," + ((Column)obj).getColumnName());
		}
		insertSql.append(") values(?");
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

