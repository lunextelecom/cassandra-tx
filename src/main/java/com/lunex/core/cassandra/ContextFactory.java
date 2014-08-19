package com.lunex.core.cassandra;

import com.google.common.base.Strings;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.lunex.core.utils.Configuration;
import com.lunex.core.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * The Class ContextFactory.
 */
public class ContextFactory {

	/** The Constant logger. */
	private static final Logger logger = LoggerFactory.getLogger(ContextFactory.class);

	/** The instance. */
	private static ContextFactory instance;
	
	/** The map org tx. */
	private static HashMap<String, String> mapOrgTX = new HashMap<String, String>();
	
	/** The map table metadata. */
	private static HashMap<String, TableMetadata> mapTableMetadata = new HashMap<String, TableMetadata>();
	
	/** The cluster. */
	private Cluster cluster;
	
	/** The session. */
	private Session session;

	/**
	 * Gets the map org tx.
	 *
	 * @return HashMap<String, String>
	 */
	public static HashMap<String, String> getMapOrgTX() {
		return mapOrgTX;
	}

	/**
	 * Gets the map table metadata.
	 *
	 * @return HashMap<String, TableMetadata> 
	 */
	public static HashMap<String, TableMetadata> getMapTableMetadata() {
		return mapTableMetadata;
	}

	/**
	 * Inits the.
	 *
	 * @param node 
	 * @param port 
	 * @param keyspace : original keyspace
	 * @param txKeyspace : transaction keyspace(contains temporary table) 
	 */
	public static void init(final String node, final int port,
			final String keyspace, final String txKeyspace) {
		logger.info("init ContextFactory with node: " + node, ", port:" + port,
				", keyspace: " + keyspace, ", txKeyspace: " + txKeyspace);
		Cluster cluster = Cluster.builder().addContactPoint(node).build();

		Session session = cluster.connect();

		Metadata metadata = cluster.getMetadata();

		KeyspaceMetadata keyspaceMetadata;
		KeyspaceMetadata txKeyspaceMetadata;
		session = cluster.connect();
		if (metadata != null) {
			keyspaceMetadata = metadata.getKeyspace(keyspace);
			if (keyspaceMetadata == null) {
				logger.error("Can't find keyspace :" + keyspace);
				throw new UnsupportedOperationException("Can't find keyspace :" + keyspace);
			}
			txKeyspaceMetadata = metadata.getKeyspace(txKeyspace);
			if (txKeyspaceMetadata == null) {
				logger.error("Can't find keyspace :" + txKeyspace);
				throw new UnsupportedOperationException("Can't find keyspace :" + txKeyspace);
			}
			for (TableMetadata child : keyspaceMetadata.getTables()) {
				// check tx table existed
				String checkSum = Utils.checkSumColumnFamily(child.getName());
				if (!Strings.isNullOrEmpty(checkSum)) {
					// check tx table
					if (child.getName().length() > Configuration.CHECKSUM_LENGTH) {
						String tCheckSum = child.getName().substring(child.getName().length() - Configuration.CHECKSUM_LENGTH, child.getName().length());
						String tName = child.getName().substring(0, child.getName().length() - (Configuration.CHECKSUM_LENGTH + 1));
						String check = Utils.checkSumColumnFamily(tName);
						if (!Strings.isNullOrEmpty(check) && check.equals(tCheckSum)) {
							// child is tx cf
							continue;
						}
					}
					String txName = child.getName() + "_" + checkSum;
					if (txKeyspaceMetadata.getTable(txName) == null) {

						// create tx column family
						StringBuilder sql = new StringBuilder(
								"CREATE TABLE " + txKeyspace + "." + txName + "(cstx_id_ uuid, cstx_deleted_ boolean, is_arith_ boolean,");
						for (ColumnMetadata col : child.getColumns()) {
							sql.append(col.getName() + " " + col.getType().toString() + ",");
						}
						sql.append(" PRIMARY KEY (cstx_id_");
						for (ColumnMetadata colKey : child.getPrimaryKey()) {
							sql.append("," + colKey.getName());
						}
						sql.append(")");
						sql.append(")");

						// create tx cf
						session.execute(sql.toString());
						logger.debug(sql.toString());
					}
					mapOrgTX.put(child.getName(), txName);

				} else {
					logger.error("checksum failed with cf: " + child.getName());
					throw new UnsupportedOperationException("checksum failed with cf: " + child.getName());
				}
				mapTableMetadata.put(child.getName(), child);
			}
			String txName = "cstx_context";
			if (txKeyspaceMetadata.getTable(txName) == null) {
				// create cstx_context in tx_keyspace if not exists
				String sql = "CREATE TABLE " + txKeyspace + ".cstx_context (contextid uuid, lstcfname set<text>, updateid timeuuid, PRIMARY KEY (contextid))";
				session.execute(sql);
				logger.debug(sql.toString());
			}
		}
		session.close();
		cluster.close();

	}

	/**
	 * Gets the instance.
	 *
	 * @return the instance
	 */
	public static ContextFactory getInstance() {
		try {
			if (instance == null) {
				instance = new ContextFactory();
				Builder builder = Cluster.builder();
				builder.addContactPoint(Configuration.getNode());

				PoolingOptions options = new PoolingOptions();
				options.setCoreConnectionsPerHost(HostDistance.LOCAL,
						options.getMaxConnectionsPerHost(HostDistance.LOCAL));
				builder.withPoolingOptions(options);

				instance.cluster = builder
						.withRetryPolicy(
								DowngradingConsistencyRetryPolicy.INSTANCE)
						.withReconnectionPolicy(
								new ConstantReconnectionPolicy(100L)).build();

				instance.session = instance.cluster.connect();

			}
		} catch (Exception e) {
			logger.error("Failed to create ContextFactory instance. Message :" + e.getMessage());
			throw new UnsupportedOperationException(
					"Failed to create ContextFactory instance" + ". Message :" + e.getMessage());
		}
		return instance;
	}

	/**
	 * Start.
	 *
	 * @return IContext
	 */
	public static IContext start() {
		try {
			String ctxId = UUID.randomUUID().toString();
			return getContext(ctxId);
		} catch (Exception e) {
			logger.error("Can't connect node: " + Configuration.getNode()
					+ " port :" + Configuration.getPort() + " keyspace:"
					+ Configuration.getKeyspace() + ". Message :"
					+ e.getMessage());
			throw new UnsupportedOperationException("Can't connect node: "
					+ Configuration.getNode() + " port :"
					+ Configuration.getPort() + " keyspace:"
					+ Configuration.getKeyspace());
		}
	}

	/**
	 * Start.
	 *
	 * @param contextId the context id
	 * @return the IContext
	 */
	public static IContext start(String contextId) {
		if (!Strings.isNullOrEmpty(contextId)) {
			Context ctx = new Context();
			ContextFactory client = ContextFactory.getInstance();
			try {
				contextId = Utils.generateContextIdFromString(contextId);
			} catch (Exception e) {
				logger.error("Can't connect node: " + Configuration.getNode()
						+ " port :" + Configuration.getPort() + " keyspace:"
						+ Configuration.getKeyspace());
				throw new UnsupportedOperationException("Can't connect node: "
						+ Configuration.getNode() + " port :"
						+ Configuration.getPort() + " keyspace:"
						+ Configuration.getKeyspace());
			}
			Boolean isExist = isExistContext(contextId);
			if (isExist) {
				throw new UnsupportedOperationException("contextId :"
						+ contextId + " is existed");
			}
			ctx.setClient(client);
			ctx.setCtxId(contextId);
			insertContextRecord(contextId, client);
			return ctx;
		}
		logger.error("contextId is null");
		throw new UnsupportedOperationException("contextId is null");

	}

	/**
	 * Gets the context.
	 *
	 * @param contextId 
	 * @return the context
	 */
	public static IContext getContext(String contextId) {
		String oldContext = contextId;
		try {
			ContextFactory client = ContextFactory.getInstance();
			contextId = Utils.generateContextIdFromString(contextId);
			Boolean isExist = isExistContext(contextId);
			if (isExist) {
				logger.error("contextId :" + oldContext + " is existed");
				throw new UnsupportedOperationException("contextId :"
						+ oldContext + " is existed");
			}
			Context ctx = new Context();
			ctx.setClient(client);
			ctx.setCtxId(contextId);
			insertContextRecord(contextId, client);

			return ctx;
		} catch (Exception e) {
			logger.error("get context:" + oldContext + " failed"
					+ ". Message :" + e.getMessage());
			throw new UnsupportedOperationException("get context:" + oldContext
					+ " failed" + ". Message :" + e.getMessage());
		}
	}

	/**
	 * Insert context record.
	 *
	 * @param contextId 
	 * @param client 
	 */
	private static void insertContextRecord(String contextId,
			ContextFactory client) {
		StringBuilder sql = new StringBuilder("update "
				+ Utils.getFullTXCF("cstx_context")
				+ " set updateid=now() where contextid = ?");
		try {
			List<Object> params = new ArrayList<Object>();
			params.add(UUID.fromString(contextId));
			client.getSession().execute(sql.toString(), params.toArray());
			logger.debug(sql.toString());
		} catch (Exception e) {
			logger.error("insertContextRecord failed :" + sql + ". Message :"
					+ e.getMessage());
			throw new UnsupportedOperationException(
					"insertContextRecord failed :" + sql + ". Message :"
							+ e.getMessage());
		}
	}

	/**
	 * Checks if is exist context.
	 *
	 * @param contextId 
	 * @return true, if checks if is exist context
	 */
	private static Boolean isExistContext(String contextId) {
		StringBuilder sql = new StringBuilder("select * from "
				+ Utils.getFullTXCF("cstx_context") + " where contextId = ?");
		try {
			ResultSet resultSet = ContextFactory.getInstance().getSession()
					.execute(sql.toString(), UUID.fromString(contextId));
			logger.debug(sql.toString());
			if (resultSet != null && !resultSet.isExhausted()) {
				if (!resultSet.isExhausted()) {
					return true;
				}
			}
		} catch (Exception e) {
			logger.error("function isExistContext(" + contextId + ") failed"
					+ ". Message :" + e.getMessage());
			throw new UnsupportedOperationException("function isExistContext("
					+ contextId + ") failed" + ". Message :" + e.getMessage());
		}
		return false;
	}

	/**
	 * Gets the session.
	 *
	 * @return the session
	 */
	public Session getSession() {
		return this.session;
	}

	/**
	 * Close cluster.
	 */
	public void close() {
		logger.info("context closed");
		cluster.close();
	}

}