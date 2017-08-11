package com.staryea.zhyy;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

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

		// 获取上次结束时间
		Connection meta_conn = null;
		try {
			meta_conn = DBConnUtil.getConn(prop.getProperty("meta_driver"), prop.getProperty("meta_url"),
					prop.getProperty("meta_username"), prop.getProperty("meta_password"));
			String last_end_time = BussinessUtil.getEndTime(meta_conn, table);

			// 排除正在执行(未执行完)
			if (!CommonUtil.isEnable(last_end_time)) {
				log.info("execute failure");
			} else {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date last_end_time_d = sdf.parse(last_end_time);
				Date next_end_time_d = new Date(last_end_time_d.getTime() + 180000);
				String next_end_time = sdf.format(next_end_time_d);

				String params[] = { "'" + last_end_time + "'", "'" + next_end_time + "'" };
				String sql = MessageFormat.format(prop.getProperty("sql"), params);
				log.info("last_end_time:" + last_end_time);
				log.info("next_end_time:" + next_end_time);
				log.info(sql);

				// 处理前回写记录表
				String now_time = sdf.format(new Date());
				BussinessUtil.firstWriteBack(meta_conn, table, last_end_time, next_end_time, now_time);

				Connection dbconn = null;
				try {
					dbconn = DBConnUtil.getConn(prop.getProperty("driver"), prop.getProperty("url"),
							prop.getProperty("username"), prop.getProperty("password"));
					KafkaProducer<String, String> producer = KafkaConnUtil.createProducer(kafka_url);
					int count = BussinessUtil.extractData(dbconn, sql, fields, splitFlag, producer, topic);
					producer.close();
					dbconn.close();

					// 处理后回写记录表
					now_time = sdf.format(new Date());
					BussinessUtil.successWriteBack(meta_conn, table, last_end_time, next_end_time, now_time, count);
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
