package com.lunex.core.cassandra;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

public class Airthmetic implements IAirthmetic {

	private static final Logger logger = LoggerFactory.getLogger(Airthmetic.class);

	private String normalType = "N";
	private String mergeType = "MS";
	private String tombstoneType = "MT";
	
	private Context ctx;
	private Boolean isUseTransaction;
	public Airthmetic(boolean isUseTransaction) {
		ctx = (Context) Context.start();
		this.isUseTransaction = isUseTransaction;
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
			valueSql.append("now(),'N','',?)");
			params.add(amount);
			statement.append(valueSql);
			if(isUseTransaction){
				ctx.execute4Airthmetic(statement.toString(),params.toArray());
			}else{
				ctx.executeNonContext(statement.toString(),params.toArray());
			}
		} catch (Exception ex) {
			logger.info("incre method failed" + ex.getMessage());
			throw new UnsupportedOperationException("incre method failed" + ex.getMessage());
		}
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
			ResultSet resultSet = ctx.executeNonContext(selectSql.toString(), params.toArray());
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
					logger.info(ctx.getCtxId() + " : " + row.getString("type") + " " + row.getString("version") + " " + row.getDecimal("amount"));
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
			logger.info(ctx.getCtxId() + " SUM : " + amount);
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
			List<Row> resultSet = ctx.execute(selectSql.toString(), params.toArray());
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
					logger.info(ctx.getCtxId() + " : " + row.getString("type") + " " + row.getString("version") + " " + row.getDecimal("amount"));
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
				BoundStatement ps =ctx.prepareStatement(statement.toString(), params);
				batch.add(ps);
				ctx.executeBatch(batch, false);
			}
			ctx.executeBatch(batch, true);
		} catch (Exception e) {
			logger.error("deleteAirthMeticRecord failed, statement : " + statement.toString()+ ". Message :" + e.getMessage());
			throw new UnsupportedOperationException("deleteAirthMeticRecord failed, statement : " + statement.toString()+ ". Message :" + e.getMessage());
		}
	}

	private void artInsertTombstone(String cf, Collection<Row> lstRow, String type, String version) {
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
					
					BoundStatement ps = ctx.prepareStatement(statement.toString(),params);
					batch.add(ps);
					ctx.executeBatch(batch, false);
				}
				ctx.executeBatch(batch, true);
			} catch (Exception e) {
				logger.error("executeBatch failed, statement : " + statement.toString()+ ". Message :" + e.getMessage());
				throw new UnsupportedOperationException("executeBatch failed, statement : " + statement.toString()+ ". Message :" + e.getMessage());
			}
		}
	}
	
	private void artInsertMergeRow(String cf, Object key, String type, String version, String column, BigDecimal amount, UUID updateId) {
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
			ctx.executeNonContext(statement.toString(), params.toArray());
		} catch (Exception ex) {
			throw new UnsupportedOperationException("insertMergeRow method failed"+ ". Message :" + ex.getMessage());
		}
	}


	public void commit() {
		ctx.commit();
	}

	public void rollback() {
		ctx.rollback();
	}
	
	public void close() {
		ctx.close();
	}

}
