package com.staryea.zhyy.util;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaConnUtil {

	public static KafkaProducer<String,String> createProducer(String url) {
		Properties props = new Properties();
		props.put("bootstrap.servers", url);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		return producer;
	}

}
