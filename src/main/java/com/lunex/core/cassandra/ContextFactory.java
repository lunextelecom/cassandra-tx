package com.lunex.core.cassandra;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Strings;
import com.lunex.core.utils.Configuration;

public class ContextFactory {
	
	private static final Logger logger = LoggerFactory.getLogger(ContextFactory.class);

	private static ContextFactory instance;
	
	/** Cassandra Cluster. */
	private Cluster cluster;

	/** Cassandra Session. */
	private Session session;
	
	private static HashMap<String, String> mapOrgTX = new HashMap<String, String>();
	
	private static HashMap<String, TableMetadata> mapTableMetadata = new HashMap<String, TableMetadata>();

	public static HashMap<String, String> getMapOrgTX() {
		return mapOrgTX;
	}

	public static HashMap<String, TableMetadata> getMapTableMetadata() {
		return mapTableMetadata;
	}

	/**
	 * Connect to Cassandra Cluster specified by provided node IP address and port number.
	 */
	public void connect(final String node, final int port) {
		logger.info("connect cassandra with node " + node, " port:" + port);
		this.cluster = Cluster.builder().addContactPoint(node).withPort(port)
				.build();
		session = cluster.connect();
		
	}

	public static void init(final String node, final int port, final String keyspace, final String txKeyspace) {
		logger.info("init ContextFactory with node: " + node, ", port:" + port, ", keyspace: " + keyspace, ", txKeyspace: " +txKeyspace);
		Cluster cluster = Cluster.builder().addContactPoint(node).withPort(port)
				.build();

		Session session = cluster.connect();
		
		Metadata metadata = cluster.getMetadata();
		
		KeyspaceMetadata keyspaceMetadata;
		KeyspaceMetadata txKeyspaceMetadata;
		session = cluster.connect();
		if(metadata != null){
			keyspaceMetadata = metadata.getKeyspace(keyspace);
			if(keyspaceMetadata == null){
				logger.error("Can't find keyspace :" + keyspace);
				throw new UnsupportedOperationException("Can't find keyspace :" + keyspace);
			}
			txKeyspaceMetadata = metadata.getKeyspace(txKeyspace);
			if(txKeyspaceMetadata == null){
				logger.error("Can't find keyspace :" + txKeyspace);
				throw new UnsupportedOperationException("Can't find keyspace :" + txKeyspace);
			}
			for (TableMetadata child : keyspaceMetadata.getTables()) {
				//check tx table existed
				String checkSum = checkSumColumnFamily(child.getName());
				if(!Strings.isNullOrEmpty(checkSum)){
					//check tx table
					if(child.getName().length() > Configuration.CHECKSUM_LENGTH){
						String tCheckSum = child.getName().substring(child.getName().length()-Configuration.CHECKSUM_LENGTH, child.getName().length());
						String tName = child.getName().substring(0, child.getName().length()-(Configuration.CHECKSUM_LENGTH+1));
						String check = checkSumColumnFamily(tName);
						if(!Strings.isNullOrEmpty(check) && check.equals(tCheckSum)){
							//child is tx cf
							continue;
						}
					}
					String txName = child.getName()+"_"+checkSum;
					if(txKeyspaceMetadata.getTable(txName) == null){
						
						//create tx column family
						StringBuilder sql = new StringBuilder("CREATE TABLE " + txKeyspace + "." + txName + "(cstx_id_ uuid, cstx_deleted_ boolean, is_arith_ boolean,");
						Boolean isFirst = true;
						for (ColumnMetadata col : child.getColumns()) {
							sql.append(col.getName() + " " + col.getType().toString() + ",");
						}
						sql.append(" PRIMARY KEY (cstx_id_");
						for (ColumnMetadata colKey : child.getPrimaryKey()) {
							sql.append("," + colKey.getName());
						}
						sql.append(")");
						sql.append(")");
						
						//
						/*String cql = child.asCQLQuery();
						cql = cql.replace("CREATE TABLE " + keyspace + "." + child.getName() + " ", "CREATE TABLE " + txKeyspace + "." + txName + " ");
						
						//
						StringBuilder oldPriTmp = new StringBuilder("PRIMARY KEY (");
						List<ColumnMetadata> lstParKey = child.getPartitionKey();
						if(lstParKey != null && lstParKey.size() >1){
							oldPriTmp.append("(");
							Boolean isFirst = true;
							for (ColumnMetadata colKey : child.getPartitionKey()) {
								if(isFirst){
									isFirst = false;
									oldPriTmp.append(colKey.getName());
								}else{
									oldPriTmp.append(", " + colKey.getName());
								}
							}
							oldPriTmp.append(")");
						}else{
							oldPriTmp.append(child.getPartitionKey().get(0).getName());
						}
						for (ColumnMetadata colKey : child.getClusteringColumns()) {
							oldPriTmp.append(", " + colKey.getName());
						}
						oldPriTmp.append(")");
						//
						StringBuilder newPri = new StringBuilder("cstx_id_ uuid, cstx_deleted_ boolean, PRIMARY KEY (cstx_id_");
						for (ColumnMetadata colKey : child.getPrimaryKey()) {
							newPri.append("," + colKey.getName());
						}
						newPri.append(")");
						cql = cql.replace(oldPriTmp, newPri);*/
						//create tx cf
						session.execute(sql.toString());
					}
					mapOrgTX.put(child.getName(), txName);
					
				}else{
					logger.error("checksum failed with cf: " + child.getName());
					throw new UnsupportedOperationException("checksum failed with cf: " + child.getName());
				}
				mapTableMetadata.put(child.getName(), child);
			}
			String txName = "cstx_context";
			if(txKeyspaceMetadata.getTable(txName) == null){
				//create cstx_context in tx_keyspace if not exists
				String sql = "CREATE TABLE " + txKeyspace + ".cstx_context (contextid uuid, lstcfname set<text>, createdid timeuuid, PRIMARY KEY (contextid))";
				session.execute(sql);
			}
		}
		session.close();
		cluster.close();
		
	}
	
	public Session getSession() {
		return this.session;
	}

	/** Close cluster. */
	public void close() {
		logger.info("cluster closed");
		cluster.close();
	}

	public static ContextFactory getInstance(){
		try {
			if(instance == null){
				instance = new ContextFactory();
			}
		} catch (Exception e) {
			logger.error("Failed to create ContextFactory instance");
			throw new UnsupportedOperationException("Failed to create ContextFactory instance");
		}
		return instance;
	}

	public static IContext start(){
		Context ctx = new Context();
		try {
			ContextFactory client = ContextFactory.getInstance();
			client.connect(Configuration.getNode(), Configuration.getPort());
			ctx.setClient(client);
			String ctxId = UUID.randomUUID().toString();
			ctx.setCtxId(ctxId);
		} catch (Exception e) {
			logger.error("Can't connect node: " + Configuration.getNode() + " port :" + Configuration.getPort() + " keyspace:" + Configuration.getKeyspace());
			throw new UnsupportedOperationException("Can't connect node: " + Configuration.getNode() + " port :" + Configuration.getPort() + " keyspace:" + Configuration.getKeyspace());
		}
		return ctx;
	}

	public static IContext start(String contextId){
		if(!Strings.isNullOrEmpty(contextId)){
			Context ctx = new Context();
			try {
				ContextFactory client = ContextFactory.getInstance();
				client.connect(Configuration.getNode(), Configuration.getPort());
				HashSet<String> currentCTX = getCurrentContext(client);
				contextId = generateContextIdFromString(contextId);
				if(currentCTX.contains(contextId)){
					throw new UnsupportedOperationException("contextId :" + contextId + " is existed");
				}
				ctx.setClient(client);
				ctx.setCtxId(contextId);
			} catch (Exception e) {
				logger.error("Can't connect node: " + Configuration.getNode() + " port :" + Configuration.getPort() + " keyspace:" + Configuration.getKeyspace());
				throw new UnsupportedOperationException("Can't connect node: " + Configuration.getNode() + " port :" + Configuration.getPort() + " keyspace:" + Configuration.getKeyspace());
			}
			return ctx;
		}
		logger.error("contextId is null");
		throw new UnsupportedOperationException("contextId is null");
		
	}

	public static IContext getContext(String contextId){
		try {
			ContextFactory client = ContextFactory.getInstance();
			client.connect(Configuration.getNode(), Configuration.getPort());
			contextId = generateContextIdFromString(contextId);
			HashSet<String> currentCTX = getCurrentContext(client);
			if(currentCTX.contains(contextId)){
				Context ctx = new Context();
				ctx.setClient(client);
				ctx.setCtxId(contextId);
				return ctx;
			}
		} catch (Exception e) {
			logger.error("contextId :" + contextId + " does not exist");
			throw new UnsupportedOperationException("contextId :" + contextId + " does not exist");
		}
		return null;
	}
	
	private static HashSet<String> getCurrentContext(ContextFactory client){
		HashSet<String> res = new HashSet<String>();
		StringBuilder sql = new StringBuilder("select * from " + Configuration.getTxKeyspace() + "." + "cstx_context");
		try {
			ResultSet resultSet = client.getSession().execute(sql.toString());
			if (resultSet != null && !resultSet.isExhausted()) {
				while(!resultSet.isExhausted()){
					final Row row = resultSet.one();
					res.add(row.getUUID("contextId").toString());
				}
			}
		} catch (Exception e) {
			logger.error("getTablesChanged failed :" + sql);
			throw new UnsupportedOperationException("getTablesChanged failed :" + sql);
		}
		return res;
	}

	public static String generateMD5Hash(String input) throws Exception {
		MessageDigest md = MessageDigest.getInstance("MD5");
		md.update(input.getBytes());

		byte byteData[] = md.digest();

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < byteData.length; i++) {
			sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16)
					.substring(1));
		}
		return sb.toString();
	}
	
	public static String generateContextIdFromString(String input) throws Exception {
		try{
			UUID.fromString(input);
		}catch(Exception ex){
			StringBuffer sb = new StringBuffer(generateMD5Hash(input));
			for(int i = 8; i < 24; i=i+5){
				sb.insert(i, "-");
			}
			return sb.toString();
		}
		return input;
	}


	public static String checkSumColumnFamily(String cfName) {
		StringBuffer sb;
		try {
			sb = new StringBuffer(generateMD5Hash(cfName));
			return sb.toString().substring(0, Configuration.CHECKSUM_LENGTH);
		} catch (Exception e) {
			return null;
		}
	}

}