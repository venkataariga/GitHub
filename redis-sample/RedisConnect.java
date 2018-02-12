package com.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;

public class RedisConnect {
	
	 static RedisClient redisClient = null;
	 static RedisConnection<String, String> connection = null;
	 static Map<String,String> vendorCodesMap  = null;
	 static Map<String,String> VendTopicsMap = null;
	
	public static void main(String[] args) {
		
		
	    redisClient = new RedisClient(
	    RedisURI.create("redis://localhost:6379"));
	    connection  = redisClient.connect();
	    System.out.println("Connected to Redis");
	    vendorCodesMap = new HashMap<String,String>();
	    vendorCodesMap.put("ASSET_TL", "2");
	    vendorCodesMap.put("BOSCH", "1");
	    
	    VendTopicsMap = new HashMap<String,String>();
	    VendTopicsMap.put("ASSET_TL_GE", "T1");
	    VendTopicsMap.put("BOSCH_GE", "T2");
	    RedisConnect  redisConnet = new RedisConnect();
	   ArrayList<String> array =  redisConnet.getRedisRegisteredTopic("12345678912345", "GE");
	    
	  System.out.println(array);
	    

	    connection.close();
	    redisClient.shutdown();
	  }
	
	public ArrayList<String> getRedisRegisteredTopic(String imei,String evtTyp) {
		ArrayList<String> topList = new ArrayList<String>();
		String venCode = connection.get(imei);
		System.out.println(venCode);
		int vendCodeInt = Integer.parseInt(venCode);
		
		for(Map.Entry<String, String> entry : vendorCodesMap.entrySet()) {
			
			int val = Integer.parseInt(entry.getValue());
			System.out.println("val"+val);
			System.out.println("vendCodeInt"+vendCodeInt);
			int logicalVal = val & vendCodeInt; 
			if(logicalVal>0) {
			System.out.println(true);
				String top = entry.getKey()+"_"+evtTyp;
				System.out.println("top"+top);
				String topicName = VendTopicsMap.get(top);
				topList.add(topicName);
				
			}
		}	
		return topList;
		 
	}


}
