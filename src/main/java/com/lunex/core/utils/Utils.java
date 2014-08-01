package com.lunex.core.utils;

import java.security.MessageDigest;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

public class Utils {

	public static String getFullTXCF(String txCF){
		return Configuration.getTxKeyspace() + "." + txCF;
	}
	
	public static String getFullOriginalCF(String originalCF){
		return Configuration.getKeyspace() + "." + originalCF;
	}

	public static int minutesDiff(UUID earlierDate, UUID laterDate)
	{
	    if( earlierDate == null || laterDate == null ) return 0;

	    return Math.abs((int)((UUIDs.unixTimestamp(laterDate)/60000) - (UUIDs.unixTimestamp(earlierDate)/60000)));
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
