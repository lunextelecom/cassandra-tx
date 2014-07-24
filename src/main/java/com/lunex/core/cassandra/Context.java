package com.lunex.core.cassandra;

import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import net.sf.jsqlparser.expression.Function;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.lunex.core.utils.Configuration;
import com.lunex.core.utils.RowKey;

public class Context implements IContext {

	private static final Logger logger = LoggerFactory.getLogger(Context.class);
	
	private String ctxId;

	private ContextFactory client;
	
	private int batchSize = 100;

	public void commit() {
		Set<String> setCfTxChanged = getTablesChange();
		if(setCfTxChanged != null && !setCfTxChanged.isEmpty()){
			StringBuilder query = new StringBuilder();
			BatchStatement batch = new BatchStatement();
			BoundStatement bs = null;
			for (String cftx : setCfTxChanged) {
				query = new StringBuilder();
				//get data from tmp table
				query.append("SELECT * from " + getFullTXCF(cftx) + " WHERE cstx_id_ = ?");
				final ResultSet results = client.getSession().execute(query.toString(), UUID.fromString(ctxId));
				if(results != null){
					while(!results.isExhausted()){
						final Row row = results.one();
						//check whether row is deleted
						Boolean isDeleted = false;
						try{
							isDeleted = row.getBool(("cstx_deleted_"));
						}catch(Exception ex){
							isDeleted = false;
						}
						if(isDeleted){
							//commit delete statement
							bs = commitDeleteStatement(cftx, row);
							if(bs != null){
								batch.add(bs);
							}
						}else{
							//commit others statement
							bs = commitOthersStatement(cftx, row);
							if(bs != null){
								batch.add(bs);
							}
						}
						executeBatch(batch, false);
					}
				}
			}
			executeBatch(batch, true);
			//discard all record involved ctxId 
			rollback();
		}
		
	}

	private String getFullTXCF(String txCF){
		return Configuration.getTxKeyspace() + "." + txCF;
	}
	
	private String getFullOriginalCF(String originalCF){
		return Configuration.getKeyspace() + "." + originalCF;
	}
	
	private BoundStatement commitOthersStatement(String cftx, final Row row) {
		StringBuilder statement;
		StringBuilder valueSql;
		//get original table from tmp table:  customer_91ec1f93 -> customer
		String originalCF = cftx.substring(0, cftx.length()-(Configuration.CHECKSUM_LENGTH+1));
		//get tablemetadata from dictionary
		TableMetadata def = ContextFactory.getMapTableMetadata().get(originalCF);
		if(def != null){
			//generation insert statement for original table
			statement = new StringBuilder();
			statement.append("insert into " + getFullOriginalCF(originalCF) + "(");
			
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
				BoundStatement ps = client.getSession().prepare(statement.toString()).bind(params.toArray());
				return ps;
			} catch (Exception e) {
				throw new UnsupportedOperationException("commit failed, statement : " + statement.toString());
			}
		}
		return null;
	}

	private BoundStatement commitDeleteStatement(String cftx, final Row row) {
		StringBuilder statement;
		//get original table from tmp table:  customer_91ec1f93 -> customer
		String originalCF = cftx.substring(0, cftx.length()-(Configuration.CHECKSUM_LENGTH+1));
		//generate delete statement
		statement = new StringBuilder();
		statement.append("delete from " + getFullOriginalCF(originalCF));
		Boolean isFirst = true;
		TableMetadata def = ContextFactory.getMapTableMetadata().get(originalCF);
		List<Object> params = new ArrayList<Object>();
		for (ColumnMetadata colKey : def.getPrimaryKey()) {
			//init primary keys in where condition
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
			BoundStatement ps = client.getSession().prepare(statement.toString()).bind(params.toArray());
			return ps;
		} catch (Exception e) {
			throw new UnsupportedOperationException("commit failed, statement : " + statement.toString());
		}
	}

	private void executeBatch(BatchStatement batch, Boolean forceRun){
		if(forceRun){
			if(batch.getStatements() != null && batch.getStatements().size()>0){
				client.getSession().execute(batch);
				batch.clear();
			}
		}else{
			if(batch.getStatements() != null && batch.getStatements().size()%batchSize==0){
				client.getSession().execute(batch);
				batch.clear();
			}
		}
	}
	public void rollback() {
		//rollback all changes, delete tmp table has cstx_id_ = ctxId
		Set<String> setCfTxChanged = getTablesChange();
		BatchStatement batch = new BatchStatement();
		BoundStatement bs = null;
		if(setCfTxChanged != null && !setCfTxChanged.isEmpty()){
			for (String cf : setCfTxChanged) {
				try {
					bs = client.getSession().prepare("delete from " + getFullTXCF(cf) + " where cstx_id_ = ?").bind(UUID.fromString(ctxId));
					if(bs != null){
						batch.add(bs);
					}
					executeBatch(batch, false);
				} catch (Exception e) {
					throw new UnsupportedOperationException("rollback failed");
				}
			}
			bs = client.getSession().prepare("delete from " + getFullTXCF("cstx_context")  + " where contextid = ?").bind(UUID.fromString(ctxId));
			if(bs != null){
				batch.add(bs);
			}
			executeBatch(batch, true);
		}
	}
	public void merge(String cf, Object key, String mergedColumn) {
		/* key : partitionkey(required) + clustering key except column with timeuuid type
		 * 1. lstRec = get all record of cf by key
		 * 2. sum lstRec by  mergedCoulmn
		 * 3. update lstRec with isMerged = true
		 * 4. insert sum record 
		 * 5. delete lstRec
		 * */
		
		//1. lstRec = get all record of cf by key
		int index = cf.indexOf(".");
		if(index != -1){
			cf = cf.substring(cf.indexOf(".")+1);
		}
		TableMetadata def = ContextFactory.getMapTableMetadata().get(cf);
		StringBuilder selectSql = new StringBuilder("select * from " + getFullOriginalCF(cf) + " where ");
		List<ColumnMetadata> lstPrimaryKey = def.getPrimaryKey(); 
		List<Object> params = new ArrayList<Object>();
		if(lstPrimaryKey != null && lstPrimaryKey.size() >0){
			if(lstPrimaryKey.size()==1){
				selectSql.append(lstPrimaryKey.get(0).getName() + " = ?");
				params.add(key);
			}else{
				Boolean isFirst = true;
				Map<String, Object> mapKey = (Map<String, Object>) key;
				for (ColumnMetadata col : lstPrimaryKey) {
					if(!col.getType().getName().toString().equals(DataType.timeuuid().getName().toString())){
						if(isFirst){
							isFirst = false;
							selectSql.append(col.getName() + " = ?");
						}else{
							selectSql.append(" and " + col.getName() + " = ?");
						}
						params.add(mapKey.get(col.getName()));
					}
				}
			}
		}
		//2. sum lstRec by  mergedCoulmn
		List<Row> lstRec = new ArrayList<Row>();
		BigDecimal amount = new BigDecimal(0);
		ResultSet resultSet = client.getSession().execute(selectSql.toString(),params.toArray());
		if(resultSet != null){
			while(!resultSet.isExhausted()){
				Row row = resultSet.one();
				lstRec.add(row);
				if(!row.getBool("isMerged")){
					amount = amount.add(row.getDecimal(mergedColumn));
				}
			}
		}
		
		//3. update lstRec with isMerged = true
		StringBuilder updateSql = new StringBuilder("update " + getFullOriginalCF(cf) + " set isMerged = true where ");
		if(lstPrimaryKey != null && lstPrimaryKey.size() >0){
			if(lstPrimaryKey.size()==1){
				updateSql.append(lstPrimaryKey.get(0).getName() + " = ?");
			}else{
				Boolean isFirst = true;
				for (ColumnMetadata col : lstPrimaryKey) {
					if(isFirst){
						isFirst = false;
						updateSql.append(col.getName() + " = ?");
					}else{
						updateSql.append(" and " + col.getName() + " = ?");
					}
				}
			}
		}
		BatchStatement batch = new BatchStatement();
		PreparedStatement updateStatement = client.getSession().prepare(updateSql.toString());
		for (Row row : lstRec) {
			params = new ArrayList<Object>();
			for (ColumnMetadata col : lstPrimaryKey) {
				params.add(RowKey.getValue(row, col.getType().getName().toString(), col.getName()));
			}
			batch.add(updateStatement.bind(params.toArray()));
			executeBatch(batch, false);
		}
		executeBatch(batch, true);
		//4. insert sum record 
		StringBuilder insertSql = new StringBuilder("insert into " + getFullOriginalCF(cf) +"(");
		StringBuilder valueSql = new StringBuilder(" values(");
		params = new ArrayList<Object>();
		Boolean isFirst = true;
		if(lstPrimaryKey != null && lstPrimaryKey.size() >0){
			if(lstPrimaryKey.size()==1){
				insertSql.append(lstPrimaryKey.get(0).getName());
				valueSql.append("?");
				params.add(key);
			}else{
				Map<String, Object> mapKey = (Map<String, Object>) key;
				for (ColumnMetadata col : lstPrimaryKey) {
					if(isFirst){
						isFirst = false;
						insertSql.append(col.getName());
						valueSql.append("?");
					}else{
						insertSql.append("," + col.getName());
						valueSql.append(",?");
					}
					if(!col.getType().getName().toString().equals(DataType.timeuuid().getName().toString())){
						params.add(mapKey.get(col.getName()));
					}else{
						valueSql.deleteCharAt(valueSql.length()-1);
						valueSql.append("now()");
					}
				}
			}
		}
		insertSql.append("," + mergedColumn);
		valueSql.append(",?");
		params.add(amount);
		insertSql.append(")");
		valueSql.append(")");
		insertSql.append(valueSql);
		client.getSession().execute(insertSql.toString(), params.toArray());
		
		
		//5. delete lstRec
		StringBuilder deleteSql = new StringBuilder("delete from " + getFullOriginalCF(cf) + "  where ");
		if(lstPrimaryKey != null && lstPrimaryKey.size() >0){
			if(lstPrimaryKey.size()==1){
				deleteSql.append(lstPrimaryKey.get(0).getName() + " = ?");
			}else{
				isFirst = true;
				for (ColumnMetadata col : lstPrimaryKey) {
					if(isFirst){
						isFirst = false;
						deleteSql.append(col.getName() + " = ?");
					}else{
						deleteSql.append(" and " + col.getName() + " = ?");
					}
				}
			}
		}
		batch = new BatchStatement();
		PreparedStatement deleteStatement = client.getSession().prepare(deleteSql.toString());
		for (Row row : lstRec) {
			params = new ArrayList<Object>();
			for (ColumnMetadata col : lstPrimaryKey) {
				params.add(RowKey.getValue(row, col.getType().getName().toString(), col.getName()));
			}
			batch.add(deleteStatement.bind(params.toArray()));
			executeBatch(batch, false);
		}
		executeBatch(batch, true);
		
	}
	
	public BigDecimal sum(String cf, Object key, String sumColumn) {
		/* key : partitionkey(required) + clustering(optional)
		 * 1. lstRec = get all record of cf by key
		 * 2. sum lstRec by  sumColumn
		 * */
		
		//1. lstRec = get all record of cf by key
		int index = cf.indexOf(".");
		if(index != -1){
			cf = cf.substring(cf.indexOf(".")+1);
		}
		TableMetadata def = ContextFactory.getMapTableMetadata().get(cf);
		StringBuilder selectSql = new StringBuilder("select " + sumColumn + " from " + getFullOriginalCF(cf) + " where ");
		List<ColumnMetadata> lstPrimaryKey = def.getPrimaryKey(); 
		List<Object> params = new ArrayList<Object>();
		if(lstPrimaryKey != null && lstPrimaryKey.size() >0){
			if(lstPrimaryKey.size()==1){
				selectSql.append(lstPrimaryKey.get(0).getName() + " = ?");
				params.add(key);
			}else{
				Boolean isFirst = true;
				Map<String, Object> mapKey = (Map<String, Object>) key;
				for (ColumnMetadata col : lstPrimaryKey) {
					if(mapKey.get(col.getName()) != null){
						if(isFirst){
							isFirst = false;
							selectSql.append(col.getName() + " = ?");
						}else{
							selectSql.append(" and " + col.getName() + " = ?");
						}
						params.add(mapKey.get(col.getName()));
					}
				}
			}
		}
		//2. sum lstRec 
		BigDecimal amount = new BigDecimal(0);
		List<Row> resultSet = execute(selectSql.toString(), params.toArray());
		if(resultSet != null){
			for (Row row : resultSet) {
				if(!row.getBool("isMerged")){
					amount = amount.add(row.getDecimal(sumColumn));
				}
			}
		}
		return amount;
		
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
			Set<String> setCfTxChanged = getTablesChange();
			if (stm instanceof Select) {
				//select statement
				sql = sql.toLowerCase();
				String tableName = ((PlainSelect)((Select) stm).getSelectBody()).getFromItem().toString().toLowerCase();
				int index = tableName.indexOf(Configuration.getKeyspace().toLowerCase() + ".");
				if(index == -1){
					//input doesnot contain keyspace
					sql = sql.replaceFirst(tableName, getFullOriginalCF(tableName));
				}else{
					tableName = tableName.replaceFirst(Configuration.getKeyspace().toLowerCase()+".", "");
				}
				res = executesSelectStatement(ctxId, sql, tableName, args);

			} else if (stm instanceof Update) {
				//update statement
				String tableName = ((Update) stm).getTable().getName();
				updateTablesChange(setCfTxChanged, ContextFactory.getMapOrgTX().get(tableName));
				executeUpdateStatement(sql, (Update) stm, args);

			} else if (stm instanceof Delete) {
				//delete statement
				String tableName = ((Delete) stm).getTable().getName();
				updateTablesChange(setCfTxChanged, ContextFactory.getMapOrgTX().get(tableName));
				executeDeleteStatement(ctxId, sql, (Delete) stm, args);

			} else if (stm instanceof Insert) {
				//insert statement
				String tableName = ((Insert) stm).getTable().getName();
				updateTablesChange(setCfTxChanged, ContextFactory.getMapOrgTX().get(tableName));
				executeInsertStatement(ctxId, (Insert) stm, args);
			}
		} catch (Exception e) {
			throw new UnsupportedOperationException();
		}
		return res;
		
	}

	private void executeUpdateStatement(String sql, Update info, 
			Object... args) {
		/*
		 * 1. generate select statement
		 * 2. insert into tmp table
		 * 3. execute update statement on tmp table
		 * */
		String orgName = info.getTable().getName();
		String txName = ContextFactory.getMapOrgTX().get(orgName);
		//1. generate select statement
		Boolean isReplace = false;
		String tmpString ="_fixedJSqlParserBug_";
		for (Object obj : info.getExpressions()) {
			if(obj instanceof Function){
				if(((Function) obj).getParameters() == null){
					ExpressionList expr = new ExpressionList();
					List<Object> listparam= new ArrayList<Object>();
					listparam.add(tmpString);
					expr.setExpressions(listparam);
					((Function)obj).setParameters(expr);
					isReplace = true;
				}
				
			}
		}
		String selectSql = null;
		
		if(isReplace){
			selectSql = info.toString().replaceAll(tmpString, "").toLowerCase();
		}else{
			selectSql = info.toString().toLowerCase();
		}
		selectSql = selectSql.replaceFirst("update ", "select * from ");
		List<Row> lstRow = executesSelectStatement(ctxId, selectSql, orgName, args);
		//2. insert into tmp table
		BatchStatement batch = new BatchStatement();
		BoundStatement bs = null;
		if(lstRow != null && !lstRow.isEmpty()){
			List<Object> params = new ArrayList<Object>();
			StringBuilder insertSql = new StringBuilder();
			StringBuilder valueSql = new StringBuilder();
			TableMetadata def = ContextFactory.getMapTableMetadata().get(orgName);
			for (Row row : lstRow) {
				
				params = new ArrayList<Object>();
				insertSql = new StringBuilder();
				valueSql = new StringBuilder();
				insertSql.append("insert into " + getFullTXCF(txName) + "(cstx_id_");
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
				bs = client.getSession().prepare(insertSql.toString()).bind(params.toArray());
				if(bs != null){
					batch.add(bs);
				}
				executeBatch(batch, false);
			}
		}
			
		//3. execute update statement on tmp table
		StringBuilder updateSql = new StringBuilder(sql.replaceFirst(info.getTable().getWholeTableName(), getFullTXCF(txName)) + " and cstx_id_ = ?");
		List<Object> params = new ArrayList<Object>();
		for(int i = 0; i < args.length; i++){
			params.add(args[i]);
		}
		params.add(UUID.fromString(ctxId));
		bs = client.getSession().prepare(updateSql.toString()).bind(params.toArray());
		if(bs != null){
			batch.add(bs);
		}
		executeBatch(batch, true);
	}
	
	private List<Row> executesSelectStatement(String contextId, String sql, String tableName, Object... args){
		/* 1. get data from original table
		 * 2. get data from tmp table
		 * 3. return data from tmp table if exists, otherwise return data from original table
		 * */
		
		//1. get data from original table
		ResultSet results = client.getSession().execute(sql, args);
		List<Row> lstOrg = new ArrayList<Row>();
		if(results != null){
			while(!results.isExhausted()){
				lstOrg.add(results.one());
			}
		}
		//2. get data from tmp table
		TableMetadata def = ContextFactory.getMapTableMetadata().get(tableName);
		String txName = ContextFactory.getMapOrgTX().get(tableName);
		StringBuilder txSql = new StringBuilder(sql.toString().replaceFirst(getFullOriginalCF(tableName), getFullTXCF(txName))); 
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
		//3. return data from tmp table if exists, otherwise return data from original table
		List<Row> res = new ArrayList<Row>(); 
		for (Row row : mapRow.values()) {
			res.add(row);
		}
		if(lstOrg != null && lstOrg.size() > 0){
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
		}
		return res;
	}
	
	private void executeDeleteStatement(String contextId, String sql, Delete info, Object... args){
		/*
		 * 1. select data from original table
		 * 2. insert into tmp table with cstx_deleted_ = true
		 * */
		
		//1. select data from original table
		sql = sql.toLowerCase();
		sql = sql.replaceFirst("delete ", "select * ");
		final ResultSet results = client.getSession().execute(sql, args);
		
		//2. insert into tmp table with cstx_deleted_ = true
		BatchStatement batch = new BatchStatement();
		BoundStatement bs = null;
		if(results != null){
			String txName = ContextFactory.getMapOrgTX().get(info.getTable().getName());
			
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
					insertSql.append("insert into " +getFullTXCF(txName) + "(cstx_id_, cstx_deleted_");
					valueSql.append(" values(?, true");
					params.add(UUID.fromString(contextId));
					for (ColumnMetadata col : def.getPrimaryKey()) {
						insertSql.append("," + col.getName());
						valueSql.append(",?");
						params.add(RowKey.getValue(row, col.getType().getName().toString(), col.getName()));
					}
					insertSql.append(")");
					valueSql.append(")");
					insertSql.append(valueSql);
					bs = client.getSession().prepare(insertSql.toString()).bind(params.toArray());
					if(bs != null){
						batch.add(bs);
					}
					executeBatch(batch, false);
				}
			}
			executeBatch(batch, true);
		}
	}
	
	private void executeInsertStatement(String contextId, Insert info, Object... args){
		logger.info("executeInsertStatement");
		
		 /* generate insert statement from tmp table
		 */ 
		String orgName = info.getTable().getName();
		String txName = ContextFactory.getMapOrgTX().get(orgName);
		info.getTable().setName(txName);
		info.getTable().setSchemaName(Configuration.getTxKeyspace());
		info.getColumns().add("cstx_id_");
		
		List<Object> params = new ArrayList<Object>();
		for(int i = 0; i < args.length; i++){
			params.add(args[i]);
		}
		params.add(UUID.fromString(ctxId));
		Boolean isReplace = false;
		String tmpString ="_fixedJSqlParserBug_";
		for (Object obj : ((ExpressionList) info.getItemsList()).getExpressions()) {
			if(obj instanceof Function){
				if(((Function) obj).getParameters() == null){
					ExpressionList expr = new ExpressionList();
					List<Object> listparam= new ArrayList<Object>();
					listparam.add(tmpString);
					expr.setExpressions(listparam);
					((Function)obj).setParameters(expr);
					isReplace = true;
				}
				
			}
		}
		
		StringBuilder insertSql = null;
		
		if(isReplace){
			insertSql = new StringBuilder(info.toString().replaceAll(tmpString, ""));
		}else{
			insertSql = new StringBuilder(info.toString());
		}
		insertSql = insertSql.replace(insertSql.lastIndexOf(")"), insertSql.length(), ",?)");
		client.getSession().execute(insertSql.toString(), params.toArray());
	}
	
	private void updateTablesChange(Set<String> input, String tableName){
		if(!input.contains(tableName)){
			input.add(tableName);
			StringBuilder sql = new StringBuilder("update " + getFullTXCF("cstx_context") + " set lstcfname = ? where contextid = ?");
			try {
				List<Object> params = new ArrayList<Object>();
				params.add(input);
				params.add(UUID.fromString(ctxId));
				client.getSession().execute(sql.toString(), params.toArray());
			} catch (Exception e) {
				throw new UnsupportedOperationException("updateTablesChange failed :" + sql);
			}
		}
	}
	
	private Set<String> getTablesChange(){
		Set<String> res = new HashSet<String>();
		StringBuilder sql = new StringBuilder("select * from " + getFullTXCF("cstx_context") + " where contextid = ?");
		try {
			List<Object> params = new ArrayList<Object>();
			params.add(UUID.fromString(ctxId));
			ResultSet resultSet = client.getSession().execute(sql.toString(), params.toArray());
			if (resultSet != null && !resultSet.isExhausted()) {
				final Row row = resultSet.one();
				res = row.getSet("lstcfname", String.class);
			}
		} catch (Exception e) {
			throw new UnsupportedOperationException("getTablesChange failed :" + sql);
		}
		return res;
	}

	public void incre(String cf, Object key, String column, BigDecimal amount){
		try {
			StringBuilder statement = new StringBuilder();
			StringBuilder valueSql = new StringBuilder();
			cf = cf.replaceFirst(Configuration.getKeyspace() + ".","");
			TableMetadata def = ContextFactory.getMapTableMetadata().get(cf);
			if (def == null) {
				throw new Exception( "Exception : can not get getMapTableMetadata() with " + cf);
			}
			statement.append("insert into " + cf + "(");
			valueSql.append(" values(");
			Boolean isHashMap = false;
			HashMap<String, Object> primaryKey = null;
			List<Object> params = new ArrayList<Object>();
			if(key instanceof HashMap<?, ?>){
				isHashMap = true;
				primaryKey = (HashMap<String, Object>) key;
			}
			if (isHashMap  && primaryKey != null) {
				for (ColumnMetadata colKey : def.getPrimaryKey()) {
					String colType = colKey.getType().getName().toString();
					if (colType.equals(DataType.timestamp().getName().toString())
							|| colType.equals(DataType.timeuuid().getName().toString())) {
						valueSql.append("now(),");
					} else {
						valueSql.append("?,");
						params.add(primaryKey.get(colKey.getName()));
					}
					statement.append(colKey.getName() + ",");
				}
			} else {
				for (ColumnMetadata colKey : def.getPrimaryKey()) {
					String colType = colKey.getType().getName().toString();
					statement.append(colKey.getName() + ",");
					if (colType.equals(DataType.timestamp().getName().toString())
							|| colType.equals(DataType.timeuuid().getName().toString())) {
						valueSql.append("now(),");
					} else {
						valueSql.append("?,");
						params.add(key);
					}
				}
			}
			statement.append(column + ")");
			valueSql.append("?)");
			params.add(amount);
			statement.append(valueSql);
			execute(statement.toString(), params.toArray());
		} catch (Exception ex) {
			throw new UnsupportedOperationException("incre method failed");
		}
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

