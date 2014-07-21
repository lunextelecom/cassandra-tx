package com.lunex.core.cassandra;

import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Strings;
import com.lunex.core.utils.Configuration;

public class ContextFactory {
	
	private static ContextFactory instance;
	
	private static Map<String, IContext> currentCTX = new HashMap<String, IContext>();
	
	/** Cassandra Cluster. */
	private Cluster cluster;

	/** Cassandra Session. */
	private Session session;
	
	private static HashMap<String, String> mapOrgTX = new HashMap<String, String>();
	
	private static HashMap<String, TableMetadata> mapTableMetadata = new HashMap<String, TableMetadata>();

	public static Map<String, IContext> getCurrentCTX() {
		return currentCTX;
	}

	public static HashMap<String, String> getMapOrgTX() {
		return mapOrgTX;
	}

	public static HashMap<String, TableMetadata> getMapTableMetadata() {
		return mapTableMetadata;
	}

	/**
	 * Connect to Cassandra Cluster specified by provided node IP address and
	 * port number.
	 *
	 * @param node
	 *            Cluster node IP address.
	 * @param port
	 *            Port of cluster host.
	 */
	public void connect(final String node, final int port, final String keyspace) {
		this.cluster = Cluster.builder().addContactPoint(node).withPort(port)
				.build();
		session = cluster.connect(keyspace);
		
	}

	public static void init(final String node, final int port, final String keyspace) {
		Cluster cluster = Cluster.builder().addContactPoint(node).withPort(port)
				.build();

		Session session = cluster.connect();
		
		Metadata metadata = cluster.getMetadata();
		
		KeyspaceMetadata keyspaceMetadata;
		session = cluster.connect();
		if(metadata != null){
			keyspaceMetadata = metadata.getKeyspace(keyspace);
			if(keyspaceMetadata == null){
				throw new UnsupportedOperationException("Can't find keyspace");
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
					if(keyspaceMetadata.getTable(txName) == null){
						//create tx column family
						String cql = child.asCQLQuery();
						cql = cql.replace("CREATE TABLE " + keyspace + "." + child.getName() + " ", "CREATE TABLE " + keyspace + "." + txName + " ");
						
						int beginIndex = cql.indexOf("PRIMARY KEY (");
						int endIndex = cql.indexOf (")",beginIndex);
						String oldPri = cql.substring(beginIndex, endIndex+1);
						StringBuilder newPri = new StringBuilder("cstx_id_ uuid, cstx_deleted_ boolean, PRIMARY KEY (cstx_id_");
						for (ColumnMetadata colKey : child.getPrimaryKey()) {
							newPri.append("," + colKey.getName());
						}
						newPri.append(")");
						cql = cql.replace(oldPri, newPri);
						//create tx cf
						session.execute(cql);
					}
					mapOrgTX.put(child.getName(), txName);
					
				}else{
					throw new UnsupportedOperationException("checksum failed with cf: " + child.getName());
				}
				mapTableMetadata.put(child.getName(), child);
			}
		}
		session.close();
		cluster.close();
		
	}
	
	/**
	 * Get Session.
	 *
	 * @return session.
	 */
	public Session getSession() {
		return this.session;
	}

	/** Close cluster. */
	public void close() {
		cluster.close();
	}

	public static ContextFactory getInstance(){
		try {
			if(instance == null){
				instance = new ContextFactory();
			}
		} catch (Exception e) {
			throw new UnsupportedOperationException("Failed to create ContextFactory instance");
		}
		return instance;
	}

	public static IContext start(){
		Context ctx = new Context();
		try {
			ContextFactory client = ContextFactory.getInstance();
			client.connect(Configuration.getNode(), Configuration.getPort(), Configuration.getKeyspace());
			ctx.setClient(client);
			String ctxId = UUID.randomUUID().toString();
			ctx.setCtxId(ctxId);
			currentCTX.put(ctxId, ctx);
		} catch (Exception e) {
			throw new UnsupportedOperationException("Can't connect node: " + Configuration.getNode() + " port :" + Configuration.getPort() + " keyspace:" + Configuration.getKeyspace());
		}
		return ctx;
	}

	public static IContext start(String contextId){
		if(!Strings.isNullOrEmpty(contextId)){
			Context ctx = new Context();
			try {
				contextId = generateContextIdFromString(contextId);
				if(currentCTX.containsKey(contextId)){
					throw new UnsupportedOperationException("contextId :" + contextId + " is existed");
				}
				ContextFactory client = ContextFactory.getInstance();
				client.connect(Configuration.getNode(), Configuration.getPort(), Configuration.getKeyspace());
				ctx.setClient(client);
				ctx.setCtxId(contextId);
				currentCTX.put(contextId, ctx);
			} catch (Exception e) {
				throw new UnsupportedOperationException("Can't connect node: " + Configuration.getNode() + " port :" + Configuration.getPort() + " keyspace:" + Configuration.getKeyspace());
			}
			return ctx;
		}
		throw new UnsupportedOperationException("contextId is null");
		
	}

	public static IContext getContext(String contextId){
		try {
			contextId = generateContextIdFromString(contextId);
			if(currentCTX.containsKey(contextId)){
				return currentCTX.get(contextId);
			}
		} catch (Exception e) {
			throw new UnsupportedOperationException("contextId :" + contextId + " does not exist");
		}
		return null;
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
		StringBuffer sb = new StringBuffer(generateMD5Hash(input));
		for(int i = 8; i < 24; i=i+5){
			sb.insert(i, "-");
		}
		return sb.toString();
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