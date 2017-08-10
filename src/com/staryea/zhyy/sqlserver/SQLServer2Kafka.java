package com.staryea.zhyy.sqlserver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.PropertyConfigurator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.staryea.zhyy.util.DBConnUtil;
import com.staryea.zhyy.util.KafkaConnUtil;
import com.staryea.zhyy.util.PropertiesUtil;

public class SQLServer2Kafka {
	private static final Logger log = LoggerFactory.getLogger(SQLServer2Kafka.class);

	public SQLServer2Kafka() {

	}

	public static void main(String[] args) {
//		String log_conf = System.getProperty("user.dir") + "/config/sqlserver/log4j.properties";
		String log_conf = args[0];
		PropertyConfigurator.configure(log_conf);

		String jdbc_conf = args[1];
//		String jdbc_conf = System.getProperty("user.dir") + "/config/sqlserver/conf.properties";
		PropertiesUtil prop = PropertiesUtil.getInstance(jdbc_conf);

		log.info(jdbc_conf);
		log.info("begin");
		
		String sql = prop.getProperty("sql");
		String kafka_url = prop.getProperty("kafka_url");
		String splitFlag = prop.getProperty("splitFlag");
		String[] fields = prop.getProperty("fields").split(",");
		String topic = prop.getProperty("topic");

		Connection dbconn = null;
		try {
			dbconn = initConn(prop);
			KafkaProducer<String, String> producer = KafkaConnUtil.createProducer(kafka_url);
			extractData(dbconn, sql, fields, splitFlag, producer, topic);
			producer.close();
			dbconn.close();
		} catch (Exception e) {
			log.error("deal error", e);
		}
		log.info("end");
	}

	private static Connection initConn(PropertiesUtil prop) throws Exception {
		Connection dbconn = null;
		String driver = prop.getProperty("driver");
		String url = prop.getProperty("url");
		String username = prop.getProperty("username");
		String password = prop.getProperty("password");

		dbconn = DBConnUtil.getConn(driver, url, username, password);
		return dbconn;
	}

	public static void extractData(Connection dbconn, String sql, String[] fields, String splitFlag,
			KafkaProducer<String, String> producer, String topic) throws Exception {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		int len = fields.length;

		pstmt = (PreparedStatement) dbconn.prepareStatement(sql);
		rs = pstmt.executeQuery();
		while (rs.next()) {
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < len; i++) {
				sb.append(rs.getObject(fields[i]) + splitFlag);
			}
			String msg = sb.substring(0, sb.length() - 1);
			log.info("topic:" + topic + ",msg:" + msg);
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
			producer.send(record);

		}
		rs.close();
		pstmt.close();
	}

}
