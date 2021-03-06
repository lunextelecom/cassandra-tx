package com.lunex.core.utils;

import com.lunex.core.cassandra.ContextFactory;

public class Configuration {

	private static String keyspace = "test_keyspace";
	
	private static String txKeyspace = "tx_keyspace";
	
	private static String node = "localhost";
	
	private static int port = 9042;
	
	public final static int CHECKSUM_LENGTH = 8;
	
	private static int batchSize = 100;
	
	public static void loadConfig(String iNode, int iPort, String iKeyspace, String itxKeyspace){
		node = iNode;
		port = iPort;
		keyspace = iKeyspace;
		txKeyspace = itxKeyspace;
		ContextFactory.init(iNode, iPort, iKeyspace, itxKeyspace);
	}

	//get, set
	public static int getBatchSize() {
		return batchSize;
	}
	
	public static String getNode() {
		return node;
	}
	public static int getPort() {
		return port;
	}
	public static String getKeyspace() {
		return keyspace;
	}

	public static String getTxKeyspace() {
		return txKeyspace;
	}

}
