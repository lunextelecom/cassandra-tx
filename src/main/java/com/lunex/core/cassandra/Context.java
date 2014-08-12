package com.lunex.core.cassandra;

import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
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
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.utils.UUIDs;
import com.lunex.core.utils.Configuration;
import com.lunex.core.utils.RowKey;
import com.lunex.core.utils.Utils;

public class Context implements IContext {

	private static final Logger logger = LoggerFactory.getLogger(Context.class);
	
	private String ctxId;

	private ContextFactory client;
	
	private int batchSize = 100;
	
	private String normalType = "N";
	private String mergeType = "MS";
	private String tombstoneType = "MT";

	public void commit() {
		logger.info("context commit");
		Set<String> setCfTxChanged = getTablesChange();
		if(setCfTxChanged != null && !setCfTxChanged.isEmpty()){
			StringBuilder query = new StringBuilder();
			BatchStatement batch = new BatchStatement();
			BoundStatement bs = null;
			for (String cftx : setCfTxChanged) {
				query = new StringBuilder();
				//get data from tmp table
				query.append("SELECT * from " + Utils.getFullTXCF(cftx) + " WHERE cstx_id_ = ?");
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
	
	private BoundStatement commitOthersStatement(String cftx, final Row row) {
		StringBuilder statement;
		StringBuilder valueSql;
		//get original table from tmp table:  customer_91ec1f93 -> customer
		String originalCF = cftx.substring(0, cftx.length()-(Configuration.CHECKSUM_LENGTH+1));
		Boolean isArith = row.getBool("is_arith_");
		//get tablemetadata from dictionary
		TableMetadata def = ContextFactory.getMapTableMetadata().get(originalCF);
		if(def != null){
			//generation insert statement for original table
			statement = new StringBuilder();
			statement.append("insert into " + Utils.getFullOriginalCF(originalCF) + "(");
			
			valueSql = new StringBuilder(" values(");
			
			Boolean isFirst = true;
			List<Object> params = new ArrayList<Object>();
			for (ColumnMetadata child : def.getColumns()) {
				String colType = child.getType().getName().toString();
				String colName = child.getName();
				if(isFirst){
					isFirst = false;
					statement.append(colName);
					if(isArith && colName.equalsIgnoreCase("updateid")){
						valueSql.append("now()");
					}else{
						valueSql.append("?");
						params.add(RowKey.getValue(row, colType, colName));
					}
				}else{
					statement.append("," + colName);
					if(isArith && colName.equalsIgnoreCase("updateid")){
						valueSql.append(",now()");
					}else{
						valueSql.append(",?");
						params.add(RowKey.getValue(row, colType, colName));
					}
				}
			}
			statement.append(")");
			valueSql.append(")");
			statement.append(valueSql);
			try {
				BoundStatement ps = client.getSession().prepare(statement.toString()).bind(params.toArray());
				return ps;
			} catch (Exception e) {
				logger.error("commit failed, statement : " + statement.toString()+ ". Message :" + e.getMessage());
				throw new UnsupportedOperationException("commit failed, statement : " + statement.toString()+ ". Message :" + e.getMessage());
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
		statement.append("delete from " + Utils.getFullOriginalCF(originalCF));
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
				statement.append(" and " + colName + " = ?");
			}
			params.add(RowKey.getValue(row, colType, colName));
			
		}
		try {
			BoundStatement ps = client.getSession().prepare(statement.toString()).bind(params.toArray());
			return ps;
		} catch (Exception e) {
			logger.error("commit failed, statement : " + statement.toString()+ ". Message :" + e.getMessage());
			throw new UnsupportedOperationException("commit failed, statement : " + statement.toString()+ ". Message :" + e.getMessage());
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
	
	private void discardContext(Boolean isClosed) {
		//rollback all changes, delete tmp table has cstx_id_ = ctxId
		logger.info("context rollback");
		Set<String> setCfTxChanged = getTablesChange();
		BatchStatement batch = new BatchStatement();
		BoundStatement bs = null;
		if(setCfTxChanged != null && !setCfTxChanged.isEmpty()){
			for (String cf : setCfTxChanged) {
				try {
					bs = client.getSession().prepare("delete from " + Utils.getFullTXCF(cf) + " where cstx_id_ = ?").bind(UUID.fromString(ctxId));
					if(bs != null){
						batch.add(bs);
					}
					executeBatch(batch, false);
				} catch (Exception e) {
					throw new UnsupportedOperationException("rollback failed");
				}
			}
			if(isClosed){
				bs = client.getSession().prepare("delete from " + Utils.getFullTXCF("cstx_context")  + " where contextid = ?").bind(UUID.fromString(ctxId));
				if(bs != null){
					batch.add(bs);
				}
			}
			executeBatch(batch, true);
		}
	}

	public void rollback() {
		discardContext(false);
	}
	
	public void close() {
		discardContext(true);
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
		return execute(sql, false, args);
		
	}
	
	public ResultSet executeNonContext(String sql, Object... arguments) {
		return client.getSession().execute(sql, arguments); 
	}
	
	private List<Row> execute(String sql, Boolean isArith, Object... args) {
		logger.info("context executing");
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
					sql = sql.replaceFirst(tableName, Utils.getFullOriginalCF(tableName));
				}else{
					tableName = tableName.replaceFirst(Configuration.getKeyspace().toLowerCase()+".", "");
				}
				res = executesSelectStatement(ctxId, sql, tableName, isArith, args);

			} else if (stm instanceof Update) {
				//update statement
				String tableName = ((Update) stm).getTable().getName();
				updateTablesChange(setCfTxChanged, ContextFactory.getMapOrgTX().get(tableName));
				executeUpdateStatement(sql, (Update) stm, isArith,args);

			} else if (stm instanceof Delete) {
				//delete statement
				String tableName = ((Delete) stm).getTable().getName();
				updateTablesChange(setCfTxChanged, ContextFactory.getMapOrgTX().get(tableName));
				executeDeleteStatement(ctxId, sql, (Delete) stm, args);

			} else if (stm instanceof Insert) {
				//insert statement
				String tableName = ((Insert) stm).getTable().getName();
				updateTablesChange(setCfTxChanged, ContextFactory.getMapOrgTX().get(tableName));
				executeInsertStatement(ctxId, (Insert) stm, isArith, args);
			}
		} catch (Exception e) {
			logger.error("context executed failed"+ ". Message :" + e.getMessage());
			throw new UnsupportedOperationException(". Message :" + e.getMessage());
		}
		return res;
		
	}

	private void executeUpdateStatement(String sql, Update info, Boolean isArith,
			Object... args) {
		logger.info("execute update statement");
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
		List<Row> lstRow = executesSelectStatement(ctxId, selectSql, orgName, isArith, args);
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
				insertSql.append("insert into " + Utils.getFullTXCF(txName) + "(cstx_id_,is_arith_");
				valueSql.append(" values(?,?");
				params.add(UUID.fromString(ctxId));
				params.add(isArith);
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
		StringBuilder updateSql = new StringBuilder(sql.replaceFirst(info.getTable().getWholeTableName(), Utils.getFullTXCF(txName)) + " and cstx_id_ = ?");
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
	
	private List<Row> executesSelectStatement(String contextId, String sql, String tableName, Boolean isArith, Object... args){
		logger.info("execute select statement");
		
		/* 1. get data from original table
		 * 2. get data from tmp table
		 * 3. return data from tmp table if exists, otherwise return data from original table
		 * */
		
		//1. get data from original table
		ResultSet results = client.getSession().execute(sql, args);
		List<Row> lstOrg = new ArrayList<Row>();
		if(results != null){
			while(!results.isExhausted()){
				final Row row = results.one();
				lstOrg.add(row);
				if(isArith){
					if(row.getString("version").contains("Head")){
						break;
					}
				}
			}
		}
		//2. get data from tmp table
		TableMetadata def = ContextFactory.getMapTableMetadata().get(tableName);
		String txName = ContextFactory.getMapOrgTX().get(tableName);
		StringBuilder txSql = new StringBuilder(sql.toString().replaceFirst(Utils.getFullOriginalCF(tableName), Utils.getFullTXCF(txName)));
		
		txSql.insert(txSql.indexOf("where", txSql.indexOf(txName)) + 5, " cstx_id_ = ? and ");
		List<Object> params = new ArrayList<Object>();
		params.add(UUID.fromString(contextId));
		for(int i = 0; i < args.length; i++){
			params.add(args[i]);
		}
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
		
		logger.info("execute delete statement");
		
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
					insertSql.append("insert into " +Utils.getFullTXCF(txName) + "(cstx_id_, cstx_deleted_");
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
	
	private void executeInsertStatement(String contextId, Insert info, Boolean isArith, Object... args){
		logger.info("execute insert statement");
		
		 /* generate insert statement from tmp table
		 */ 
		String orgName = info.getTable().getName();
		String txName = ContextFactory.getMapOrgTX().get(orgName);
		info.getTable().setName(txName);
		info.getTable().setSchemaName(Configuration.getTxKeyspace());
		info.getColumns().add("cstx_id_");
		info.getColumns().add("is_arith_");
		
		List<Object> params = new ArrayList<Object>();
		for(int i = 0; i < args.length; i++){
			params.add(args[i]);
		}
		params.add(UUID.fromString(ctxId));
		params.add(isArith);
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
		insertSql = insertSql.replace(insertSql.lastIndexOf(")"), insertSql.length(), ",?,?)");
		client.getSession().execute(insertSql.toString(), params.toArray());
	}
	
	private void updateTablesChange(Set<String> input, String tableName){
		if(!input.contains(tableName)){
			Set<String> lstTable = new HashSet<String>();
			for (String child : input) {
				lstTable.add(child);
			}
			lstTable.add(tableName);
			StringBuilder sql = new StringBuilder("update " + Utils.getFullTXCF("cstx_context") + " set lstcfname = ?, updateid=now() where contextid = ?");
			try {
				List<Object> params = new ArrayList<Object>();
				params.add(lstTable);
				params.add(UUID.fromString(ctxId));
				client.getSession().execute(sql.toString(), params.toArray());
			} catch (Exception e) {
				logger.error("updateTablesChange failed :" + sql+ ". Message :" + e.getMessage());
				throw new UnsupportedOperationException("updateTablesChange failed :" + sql+ ". Message :" + e.getMessage());
			}
		}
	}
	
	private Set<String> getTablesChange(){
		Set<String> res = new HashSet<String>();
		StringBuilder sql = new StringBuilder("select * from " + Utils.getFullTXCF("cstx_context") + " where contextid = ?");
		try {
			List<Object> params = new ArrayList<Object>();
			params.add(UUID.fromString(ctxId));
			ResultSet resultSet = client.getSession().execute(sql.toString(), params.toArray());
			if (resultSet != null && !resultSet.isExhausted()) {
				final Row row = resultSet.one();
				res = row.getSet("lstcfname", String.class);
			}
		} catch (Exception e) {
			logger.error("getTablesChange failed :" + sql+ ". Message :" + e.getMessage());
			throw new UnsupportedOperationException("getTablesChange failed :" + sql+ ". Message :" + e.getMessage());
		}
		return res;
	}

	public void merge(String cf, Object key, String column) {
		try {
			/**
			 * 1. (cassandra operation) normal_rows, tombstone_rows, merge_rows = get rows for sum
			 * */
			logger.info("1. (cassandra operation) normal_rows, tombstone_rows, merge_rows = get rows for sum");
			cf = cf.replaceFirst(Configuration.getKeyspace() + ".","");
			List<Object> params = new ArrayList<Object>();
			final StringBuilder selectSql = artCreateSelectStatement(cf, key, params);
			UUID lastestUpdateid = null;
			UUID lastestMergeUUID = null;
			ResultSet resultSet = client.getSession().execute(selectSql.toString(), params.toArray());
			Map<UUID, Row> mapNormal = new HashMap<UUID, Row>();
			Map<UUID, Row> mapMergeUUID = new HashMap<UUID, Row>();
			Map<String, Row> mapMergeVer = new HashMap<String, Row>();
			List<Row> lstAllTS = new ArrayList<Row>();
			List<Row> lstInvalidTS = new ArrayList<Row>();
			List<Row> lstValidTS = new ArrayList<Row>();
			
			List<Row> lstNorValidTS = new ArrayList<Row>();
			List<Row> lstMerValidTS = new ArrayList<Row>();
			boolean firstIsMerge = false;
			int numRow = 0;
			if(resultSet != null){
				while(!resultSet.isExhausted()){
					numRow++;
					final Row row = resultSet.one();
					if(lastestUpdateid == null){
						if(row.getString("type").equalsIgnoreCase(mergeType)){
							firstIsMerge = true;
						}
						lastestUpdateid =row.getUUID("updateid");
					}
					if(row.getString("type").equalsIgnoreCase(normalType)){
						mapNormal.put(row.getUUID("updateid"), row);
					}else if(row.getString("type").equalsIgnoreCase(mergeType)){
						if(mapMergeVer.size()==0){
							mapMergeVer.put(row.getString("version"), row);
							mapMergeUUID.put(row.getUUID("updateid"), row);
							lastestMergeUUID = row.getUUID("updateid");
						}else{
							lstInvalidTS.add(row);
						}
					}else{
						lstAllTS.add(row);
					}
					logger.info(ctxId + " : " + row.getString("type") + " " + row.getString("version") + " " + row.getDecimal("amount"));
				}
			}
			if(numRow<=1){
				return;
			}
			/**
			 * 2.discard invalid tombstone_rows
			 *	A invalid tombstone record is one which there is 
			 *		- no merge record with the same version or 
			 *		- no normal record with same updateid.  Invalid tombstone have zero value and is not counted during sum.
			 * */
			logger.info("2.discard invalid tombstone_rows");
			for (Row row : lstAllTS) {
				if((mapNormal.containsKey(row.getUUID("updateid")) || mapMergeUUID.containsKey(row.getUUID("updateid"))) && mapMergeVer.containsKey(row.getString("version"))){
					if(mapNormal.containsKey(row.getUUID("updateid"))){
						lstValidTS.add(row);
						lstNorValidTS.add(mapNormal.get(row.getUUID("updateid")));
					}else{
						Row tmpUUID = mapMergeUUID.get(row.getUUID("updateid"));
						Row tmpVer = mapMergeVer.get(row.getString("version"));
						if(!tmpVer.getString("version").equalsIgnoreCase(tmpUUID.getString("version")) ||
								!tmpVer.getUUID("updateid").toString().equalsIgnoreCase(tmpUUID.getUUID("updateid").toString())){
							lstValidTS.add(row);
							lstMerValidTS.add(mapMergeUUID.get(row.getUUID("updateid")));
						}
					}
				}else{
					lstInvalidTS.add(row);
				}
			}
			
			/**
			 * 3. sum = normal_rows + valid_tombstone_rows + merge_rows
			 * */
			logger.info("sum = normal_rows + valid_tombstone_rows + merge_rows");
			BigDecimal amount = new BigDecimal(0);
			for (Row row:  mapNormal.values()) {
				if(lastestMergeUUID != null){
					if(Utils.compare(lastestMergeUUID, row.getUUID("updateid"))==1){
						amount = amount.add(row.getDecimal(column));
					}
				}else{
					amount = amount.add(row.getDecimal(column));
				}
			}
			for (Row row : lstValidTS) {
				if(lastestMergeUUID != null){
					if(Utils.compare(lastestMergeUUID, row.getUUID("updateid"))==1){
						amount = amount.subtract(row.getDecimal(column));
					}
				}else{
					amount = amount.subtract(row.getDecimal(column));
				}
			}
			for (Row row:  mapMergeUUID.values()) {
				amount = amount.add(row.getDecimal(column));
			}
			logger.info(ctxId + " SUM : " + amount);
			/**
			 * 4. newversion = generate timeuuid
			 * */
			logger.info("newversion = generate timeuuid");
			String newversion = UUID.randomUUID().toString();
			
			if(!firstIsMerge){
				
				/**
				 * 5. (cassandra operation) insert tombstone for normal + merged rows with newversion
				 * */
				logger.info("5. (cassandra operation) insert tombstone for normal + merged rows with newversion");
				artInsertTombstone(cf, mapNormal.values(), tombstoneType, newversion);
				artInsertTombstone(cf, mapMergeVer.values(), tombstoneType, newversion);
				
				/**
				 * 6.(cassandra operation) insert merge record with sum and newversion. this operation make tombstone valid
				 * */
				logger.info("6.(cassandra operation) insert merge record with sum and newversion. this operation make tombstone valid");
				artInsertMergeRow(cf, key, mergeType, newversion, column, amount, lastestUpdateid);
			}
			
			
			/**
			 * 7.(cassandra operation, sometimes) delete normal and merge records with valid tombstone. Do not send this request if there isn't any records to delete.
			 * */
			logger.info("7.(cassandra operation, sometimes) delete normal and merge records with valid tombstone. Do not send this request if there isn't any records to delete.");
			
			artDeleteNormalRecord(cf, lstNorValidTS);
			artDeleteMergedRecord(cf, lstMerValidTS);
			
			/**
			 * 8.(cassandra operation, sometimes) delete invalid tombstone older than 10 mins if there are any, do not send this request if there isnt' any match
			 * */
			logger.info("8.(cassandra operation, sometimes) delete invalid tombstone older than 10 mins if there are any, do not send this request if there isnt' any match");
			List<Row> lstDelete = new ArrayList<Row>();
			for (Row row : lstInvalidTS) {
				int minutes = Utils.minutesDiff(row.getUUID("updateid"), lastestUpdateid);
				if(minutes >=10){
					lstDelete.add(row);
					
				}
			}
			artDeleteNormalRecord(cf, lstDelete);
			
		} catch (Exception e) {
			logger.error("merge failed . Message :" + e.getMessage());
			throw new UnsupportedOperationException("merge failed . Message :" + e.getMessage());
		}
	}

	public BigDecimal sum(String cf, Object key, String column) {
		try {
			/**
			 * 1. (cassandra operation) normal_rows, tombstone_rows, merge_rows = get rows for sum
			 * */
			logger.info("1. (cassandra operation) normal_rows, tombstone_rows, merge_rows = get rows for sum");
			cf = cf.replaceFirst(Configuration.getKeyspace() + ".","");
			List<Object> params = new ArrayList<Object>();
			final StringBuilder selectSql = artCreateSelectStatement(cf, key, params);
			List<Row> resultSet = execute(selectSql.toString(), params.toArray());
			Map<UUID, Row> mapNormal = new HashMap<UUID, Row>();
			Map<UUID, Row> mapMergeUUID = new HashMap<UUID, Row>();
			Map<String, Row> mapMergeVer = new HashMap<String, Row>();
			List<Row> lstAllTS = new ArrayList<Row>();
			List<Row> lstInvalidTS = new ArrayList<Row>();
			List<Row> lstValidTS = new ArrayList<Row>();
			UUID lastestMergeUUID = null;
			List<Row> lstNorValidTS = new ArrayList<Row>();
			List<Row> lstMerValidTS = new ArrayList<Row>();
			if(resultSet != null){
				for (Row row : resultSet) {
					if(row.getString("type").equalsIgnoreCase(normalType)){
						mapNormal.put(row.getUUID("updateid"), row);
					}else if(row.getString("type").equalsIgnoreCase(mergeType)){
						if(mapMergeVer.size()==0){
							mapMergeVer.put(row.getString("version"), row);
							mapMergeUUID.put(row.getUUID("updateid"), row);
							lastestMergeUUID = row.getUUID("updateid");
						}
					}else{
						lstAllTS.add(row);
					}
					logger.info(ctxId + " : " + row.getString("type") + " " + row.getString("version") + " " + row.getDecimal("amount"));
				}
			}
			/**
			 * 2.discard invalid tombstone_rows
			 *	A invalid tombstone record is one which there is 
			 *		- no merge record with the same version or 
			 *		- no normal record with same updateid.  Invalid tombstone have zero value and is not counted during sum.
			 * */
			logger.info("2.discard invalid tombstone_rows");
			for (Row row : lstAllTS) {
				if((mapNormal.containsKey(row.getUUID("updateid")) || mapMergeUUID.containsKey(row.getUUID("updateid"))) && mapMergeVer.containsKey(row.getString("version"))){
					if(mapNormal.containsKey(row.getUUID("updateid"))){
						lstValidTS.add(row);
						lstNorValidTS.add(mapNormal.get(row.getUUID("updateid")));
					}else{
						Row tmpUUID = mapMergeUUID.get(row.getUUID("updateid"));
						Row tmpVer = mapMergeVer.get(row.getString("version"));
						if(!tmpVer.getString("version").equalsIgnoreCase(tmpUUID.getString("version")) ||
								!tmpVer.getUUID("updateid").toString().equalsIgnoreCase(tmpUUID.getUUID("updateid").toString())){
							lstValidTS.add(row);
							lstMerValidTS.add(mapMergeUUID.get(row.getUUID("updateid")));
						}
					}
				}else{
					lstInvalidTS.add(row);
				}
			}
			
			/**
			 * 3. sum = normal_rows + valid_tombstone_rows + merge_rows
			 * */
			/**
			 * 3. sum = normal_rows + valid_tombstone_rows + merge_rows
			 * */
			logger.info("sum = normal_rows + valid_tombstone_rows + merge_rows");
			BigDecimal amount = new BigDecimal(0);
			for (Row row:  mapNormal.values()) {
				if(lastestMergeUUID != null){
					if(Utils.compare(lastestMergeUUID, row.getUUID("updateid"))==1){
						amount = amount.add(row.getDecimal(column));
					}
				}else{
					amount = amount.add(row.getDecimal(column));
				}
			}
			for (Row row : lstValidTS) {
				if(lastestMergeUUID != null){
					if(Utils.compare(lastestMergeUUID, row.getUUID("updateid"))==1){
						amount = amount.subtract(row.getDecimal(column));
					}
				}else{
					amount = amount.subtract(row.getDecimal(column));
				}
			}
			for (Row row:  mapMergeUUID.values()) {
				amount = amount.add(row.getDecimal(column));
			}
			return amount;
		} catch (Exception e) {
			logger.error("sum failed " + e.getMessage()+ ". Message :" + e.getMessage());
			throw new UnsupportedOperationException("sum failed " + e.getMessage()+ ". Message :" + e.getMessage());
		}
	}

	private StringBuilder artCreateSelectStatement(String cf, Object key,
			List<Object> params) {
		TableMetadata def = ContextFactory.getMapTableMetadata().get(cf);
		StringBuilder selectSql = new StringBuilder("select * from " + Utils.getFullOriginalCF(cf) + " where ");
		Boolean isHashMap = false;
		HashMap<String, Object> mapKey = null;
		if(key instanceof HashMap<?, ?>){
			isHashMap = true;
			mapKey = (HashMap<String, Object>) key;
		}
		if (!isHashMap) {
			selectSql.append(def.getPrimaryKey().get(0).getName() + " = ?");
			params.add(key);
		} else {
			Boolean isFirst = true;
			for (ColumnMetadata colKey : def.getPrimaryKey()) {
				if(!colKey.getName().equalsIgnoreCase("updateid")
					&& !colKey.getName().equalsIgnoreCase("type")
					&& !colKey.getName().equalsIgnoreCase("version")
					){
					if(isFirst){
						isFirst = false;
						selectSql.append(colKey.getName() + " = ?");
					}else{
						selectSql.append(" and " + colKey.getName() + " = ?");
					}
					params.add(mapKey.get(colKey.getName()));
				}
			}
		}
		return selectSql;
	}
	
	private void artDeleteTombstoneRecord(String cf, final List<Row> rows, String type, String version) {
		deleteAirthMeticRecord(cf, rows, type, version);
	}
	
	private void artDeleteMergedRecord(String cf, final List<Row> rows) {
		deleteAirthMeticRecord(cf, rows, null, null);
	}
	
	private void artDeleteNormalRecord(String cf, final List<Row> rows) {
		deleteAirthMeticRecord(cf, rows, null, null);
	}
	
	private void deleteAirthMeticRecord(String cf, final List<Row> rows, String type, String version) {
		if(rows == null || rows.isEmpty()){
			return;
		}
		StringBuilder statement;
		String originalCF = cf;
		//generate delete statement
		statement = new StringBuilder();
		statement.append("delete from " + Utils.getFullOriginalCF(originalCF));
		Boolean isFirst = true;
		TableMetadata def = ContextFactory.getMapTableMetadata().get(originalCF);
		List<Object> params = new ArrayList<Object>();
		for (ColumnMetadata colKey : def.getPrimaryKey()) {
			//init primary keys in where condition
			String colName = colKey.getName();
			if(isFirst){
				isFirst = false;
				statement.append(" where " + colName + " = ?");
			}else{
				statement.append(" and " + colName + " = ?");
			}
		}
		try {
			BatchStatement batch = new BatchStatement();
			for (Row row : rows) {
				params = new ArrayList<Object>();
				for (ColumnMetadata colKey : def.getPrimaryKey()) {
					//init primary keys in where condition
					String colType = colKey.getType().getName().toString();
					String colName = colKey.getName();
					Boolean isAdd = false;
					if(colName.equalsIgnoreCase("type")){
						if(type != null){
							params.add(type);
							isAdd  = true;
						}
					}else if(colName.equalsIgnoreCase("version")){
						if(version != null){
							params.add(version);
							isAdd = true;
						}
					}
					if(!isAdd){
						params.add(RowKey.getValue(row, colType, colName));
					}
				}
				BoundStatement ps = client.getSession().prepare(statement.toString()).bind(params.toArray());
				batch.add(ps);
				executeBatch(batch, false);
			}
			executeBatch(batch, true);
		} catch (Exception e) {
			logger.error("deleteAirthMeticRecord failed, statement : " + statement.toString()+ ". Message :" + e.getMessage());
			throw new UnsupportedOperationException("deleteAirthMeticRecord failed, statement : " + statement.toString()+ ". Message :" + e.getMessage());
		}
	}

	public void artInsertTombstone(String cf, Collection<Row> lstRow, String type, String version) {
		StringBuilder statement;
		StringBuilder valueSql;
		//get tablemetadata from dictionary
		TableMetadata def = ContextFactory.getMapTableMetadata().get(cf);
		if(def != null){
			//generation insert statement for original table
			statement = new StringBuilder();
			statement.append("insert into " + Utils.getFullOriginalCF(cf) + "(");
			
			valueSql = new StringBuilder(" values(");
			
			Boolean isFirst = true;
			List<Object> params = new ArrayList<Object>();
			for (ColumnMetadata child : def.getColumns()) {
				String colName = child.getName();
				if(isFirst){
					isFirst = false;
					statement.append(colName);
					valueSql.append("?");
				}else{
					statement.append("," + colName);
					valueSql.append(",?");
				}
			}
			statement.append(")");
			valueSql.append(")");
			statement.append(valueSql);
			
			BatchStatement batch = new BatchStatement();
			try {
				for (Row row : lstRow) {
					params = new ArrayList<Object>();
					for (ColumnMetadata child : def.getColumns()) {
						String colType = child.getType().getName().toString();
						String colName = child.getName();
						if(colName.equalsIgnoreCase("version")){
							params.add(version);
						}else if(colName.equalsIgnoreCase("type")){
							params.add(type);
						}else{
							params.add(RowKey.getValue(row, colType, colName));
						}
					}
					
					BoundStatement ps = client.getSession().prepare(statement.toString()).bind(params.toArray());
					batch.add(ps);
					executeBatch(batch, false);
				}
				executeBatch(batch, true);
			} catch (Exception e) {
				logger.error("executeBatch failed, statement : " + statement.toString()+ ". Message :" + e.getMessage());
				throw new UnsupportedOperationException("executeBatch failed, statement : " + statement.toString()+ ". Message :" + e.getMessage());
			}
		}
	}
	
	public void artInsertMergeRow(String cf, Object key, String type, String version, String column, BigDecimal amount, UUID updateId) {
		try {
			StringBuilder statement = new StringBuilder();
			StringBuilder valueSql = new StringBuilder();
			cf = cf.replaceFirst(Configuration.getKeyspace() + ".","");
			TableMetadata def = ContextFactory.getMapTableMetadata().get(cf);
			if (def == null) {
				throw new Exception( "Exception : can not get getMapTableMetadata() with " + cf);
			}
			statement.append("insert into " + Utils.getFullOriginalCF(cf) + "(");
			valueSql.append(" values(");
			Boolean isHashMap = false;
			HashMap<String, Object> mapKey = null;
			List<Object> params = new ArrayList<Object>();
			if(key instanceof HashMap<?, ?>){
				isHashMap = true;
				mapKey = (HashMap<String, Object>) key;
			}
			if (!isHashMap) {
				valueSql.append("?,");
				params.add(key);
				statement.append(def.getPartitionKey().get(0).getName() + ",");
			} else {
				for (ColumnMetadata colKey : def.getPrimaryKey()) {
					if(!colKey.getName().equalsIgnoreCase("updateid")
							&& !colKey.getName().equalsIgnoreCase("type")
							&& !colKey.getName().equalsIgnoreCase("version")
							){
						statement.append(colKey.getName() + ",");
						valueSql.append("?,");
						params.add(mapKey.get(colKey.getName()));
					}
				}
			}
			statement.append(" updateid, type, version, " + column + ")");
			valueSql.append("?,?,?,?)");
			params.add(updateId);
			params.add(type);
			params.add(version);
			params.add(amount);
			statement.append(valueSql);
			client.getSession().execute(statement.toString(), params.toArray());
		} catch (Exception ex) {
			throw new UnsupportedOperationException("insertMergeRow method failed"+ ". Message :" + ex.getMessage());
		}
	}
	
	public void incre(String cf, Object key, String column, BigDecimal amount) {
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
			HashMap<String, Object> mapKey = null;
			List<Object> params = new ArrayList<Object>();
			if(key instanceof HashMap<?, ?>){
				isHashMap = true;
				mapKey = (HashMap<String, Object>) key;
			}
			if (!isHashMap) {
				valueSql.append("?,");
				params.add(key);
				statement.append(def.getPartitionKey().get(0).getName() + ",");
			} else {
				for (ColumnMetadata colKey : def.getPrimaryKey()) {
					if(!colKey.getName().equalsIgnoreCase("updateid")
							&& !colKey.getName().equalsIgnoreCase("type")
							&& !colKey.getName().equalsIgnoreCase("version")
							){
						statement.append(colKey.getName() + ",");
						valueSql.append("?,");
						params.add(mapKey.get(colKey.getName()));
					}
				}
			}
			statement.append(" updateid, type, version, " + column + ")");
			valueSql.append("now(),'N','',?)");
			params.add(amount);
			statement.append(valueSql);
			execute(statement.toString(), true, params.toArray());
		} catch (Exception ex) {
			logger.info("incre method failed" + ex.getMessage());
			throw new UnsupportedOperationException("incre method failed" + ex.getMessage());
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

