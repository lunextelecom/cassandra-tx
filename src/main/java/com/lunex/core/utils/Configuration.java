package com.lunex.core.utils;

import com.lunex.core.cassandra.ContextFactory;

public class Configuration {

	private static String keyspace = "test_keyspace";
	
	private static String node = "localhost";
	
	private static int port = 9042;
	
	public static int CHECKSUM_LENGTH = 8;
	
	public static void loadConfig( String iNode, int iPort, String iKeyspace){
		node = iNode;
		port = iPort;
		keyspace = iKeyspace;
		ContextFactory.init(iNode, iPort, iKeyspace);
		
	}

	//get, set
	public static String getNode() {
		return node;
	}
	public static int getPort() {
		return port;
	}
	public static String getKeyspace() {
		return keyspace;
	}

}
