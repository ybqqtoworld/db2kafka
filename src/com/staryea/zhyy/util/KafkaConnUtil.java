package com.staryea.zhyy.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.security.JaasUtils;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;

public class KafkaConnUtil {

	public static KafkaProducer<String, String> createProducer(String url) {
		Properties props = new Properties();
		props.put("bootstrap.servers", url);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("auto.create.topics.enable", "true");

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		return producer;
	}

	/**
	 * 
	* @Title: createTopic  
	* @Description: TODO 创建topic
	* @param zk zookeeper连接地址
	* @param topic	topic
	* @return void
	 */
	public static void createTopic(String zk, String topic) {
		ZkUtils zkUtils = ZkUtils.apply(zk, 30000, 30000, JaasUtils.isZkSecurityEnabled());
		// 创建一个单分区单副本名为t1的topic
		AdminUtils.createTopic(zkUtils, "t1", 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
		zkUtils.close();
	}

	/**
	 * 
	* @Title: deleteTopic  
	* @Description: TODO 删除topic
	* @param zk
	* @param topic
	* @return void
	 */
	public static void deleteTopic(String zk, String topic) {
		ZkUtils zkUtils = ZkUtils.apply("localhost:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
		// 删除topic 't1'
		AdminUtils.deleteTopic(zkUtils, "t1");
		zkUtils.close();
	}

	/**
	 * 
	* @Title: queryTopic  
	* @Description: TODO 查询topic属性
	* @param zk zookeeper连接地址
	* @param topic topic
	* @return void
	 */
	public static void queryTopic(String zk, String topic) {
		ZkUtils zkUtils = ZkUtils.apply("localhost:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
		// 获取topic 'test'的topic属性属性
		Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "test");
		// 查询topic-level属性
		Iterator it = props.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			Object key = entry.getKey();
			Object value = entry.getValue();
			System.out.println(key + " = " + value);
		}
		zkUtils.close();
	}

	/**
	 * 
	* @Title: updateTopic  
	* @Description: TODO 更新topic属性
	* @param zk zookeeper连接地址
	* @param topic topic
	* @return void
	 */
	public static void updateTopic(String zk, String topic) {
		ZkUtils zkUtils = ZkUtils.apply("localhost:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
		Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "test");
		// 增加topic级别属性
		props.put("min.cleanable.dirty.ratio", "0.3");
		// 删除topic级别属性
		props.remove("max.message.bytes");
		// 修改topic 'test'的属性
		AdminUtils.changeTopicConfig(zkUtils, "test", props);
		zkUtils.close();
	}

	public static void main(String[] args) {

	}

}
