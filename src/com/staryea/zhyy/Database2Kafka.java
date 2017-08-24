package com.staryea.zhyy;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.staryea.zhyy.util.BussinessUtil;
import com.staryea.zhyy.util.CommonUtil;
import com.staryea.zhyy.util.DBConnUtil;
import com.staryea.zhyy.util.KafkaConnUtil;
import com.staryea.zhyy.util.PropertiesUtil;

public class Database2Kafka {
	private static final Logger log = LoggerFactory.getLogger(Database2Kafka.class);

	public Database2Kafka() {

	}

	public static void main(String[] args) {
		// 日志文件
		String log_conf = args[0];
		PropertyConfigurator.configure(log_conf);

		// 配置文件
		String conf = args[1];

		log.info("begin");
		long bt = System.currentTimeMillis();

		dealData(conf);

		long et = System.currentTimeMillis();
		log.info("end");
		log.info("usedtime:" + (et - bt) / 1000 + "second");

	}

	public static void dealData(String conf) {
		log.info("configure file:" + conf);
		PropertiesUtil prop = PropertiesUtil.getInstance(conf);
		String kafka_url = prop.getProperty("kafka_url");
		String splitFlag = prop.getProperty("splitFlag");
		String[] fields = prop.getProperty("fields").split(",");
		String topic = prop.getProperty("topic");
		String table = prop.getProperty("table");
		String start_time = prop.getProperty("start_time");
		String time_field = prop.getProperty("time_field");

		// 获取上次取数的最后一条数据的时间
		Connection meta_conn = null;
		try {
			meta_conn = DBConnUtil.getConn(prop.getProperty("meta_driver"), prop.getProperty("meta_url"),
					prop.getProperty("meta_username"), prop.getProperty("meta_password"));
			String last_record_time = BussinessUtil.getEndTime(meta_conn, table, start_time);

			// 排除正在执行(未执行完)
			if (!CommonUtil.isEnable(last_record_time)) {
				log.info("execute failure");
			} else {
				String params[] = { "'" + last_record_time + "'" };
				String sql = MessageFormat.format(prop.getProperty("sql"), params);
				log.info("last_record_time:" + last_record_time);
				log.info("format sql:" + sql);

				// 处理前回写记录表
				BussinessUtil.firstWriteBack(meta_conn, table, last_record_time);

				Connection dbconn = null;
				try {
					dbconn = DBConnUtil.getConn(prop.getProperty("driver"), prop.getProperty("url"),
							prop.getProperty("username"), prop.getProperty("password"));
					KafkaProducer<String, String> producer = KafkaConnUtil.createProducer(kafka_url);
					Map<String, String> map = BussinessUtil.extractData(dbconn, sql, fields, time_field, splitFlag,
							producer, topic);
					producer.close();
					dbconn.close();

					int count = Integer.parseInt(map.get("count"));
					if (count == 0) {
						map.put("last_record_time", last_record_time);
					}

					// 处理后回写记录表
					BussinessUtil.successWriteBack(meta_conn, table, last_record_time, map.get("last_record_time"),
							count);
				} catch (Exception e) {
					log.error("extract data error", e);
				}
			}
		} catch (Exception e) {
			log.error("connect meta database error", e);
		} finally {
			if (meta_conn != null) {
				try {
					meta_conn.close();
				} catch (SQLException e) {
					log.error("close  meta database connection error", e);
				}
			}
		}
	}

}
