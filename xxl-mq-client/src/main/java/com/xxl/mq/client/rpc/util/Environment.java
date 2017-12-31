package com.xxl.mq.client.rpc.util;

import java.util.Properties;

/**
 * 环境基类
 * @author xuxueli 2015-8-28 10:37:43
 */
public class Environment {

	public static final String ZK_BASE_PATH = "/xxl-mq";

	/**
	 * rpc service address on zookeeper, service path : /xxl-mq/rpc/registrykey01/address01
     */
	public static final String ZK_SERVICES_PATH = ZK_BASE_PATH.concat("/rpc");

	/**
	 * consumer name on zookeepr, consumerpath : /xxl-mq/consumer01/address01
     */
	public static final String ZK_CONSUMER_PATH = ZK_BASE_PATH.concat("/consumer");

	/**
	 * zk address
	 */
	public static final String ZK_ADDRESS = "127.0.0.1:2181";		// zk地址：格式	ip1:port,ip2:port,ip3:port

	
	public static void main(String[] args) {
		System.out.println(ZK_ADDRESS);
	}

}

