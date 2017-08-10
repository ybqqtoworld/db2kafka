package com.staryea.zhyy.sqlserver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.staryea.zhyy.util.BussinessUtil;
import com.staryea.zhyy.util.CommonUtil;
import com.staryea.zhyy.util.DBConnUtil;
import com.staryea.zhyy.util.KafkaConnUtil;
import com.staryea.zhyy.util.PropertiesUtil;

public class SQLServer2Kafka {
	private static final Logger log = LoggerFactory.getLogger(SQLServer2Kafka.class);

	public SQLServer2Kafka() {

	}

	public static void main(String[] args) {
		// 加载日志文件
		String log_conf = args[0];
		PropertyConfigurator.configure(log_conf);

		log.info("begin");

		// 加载配置文件
		String jdbc_conf = args[1];
		log.info(jdbc_conf);

		PropertiesUtil prop = PropertiesUtil.getInstance(jdbc_conf);
		String kafka_url = prop.getProperty("kafka_url");
		String splitFlag = prop.getProperty("splitFlag");
		String[] fields = prop.getProperty("fields").split(",");
		String topic = prop.getProperty("topic");
		String table = prop.getProperty("table");

		// 获取上次结束时间
		Connection meta_conn = null;
		try {
			meta_conn = initMateConn(prop);
			String end_time = BussinessUtil.getEndTime(meta_conn, table);
			// 排除正在执行(未执行完)
			if (!CommonUtil.isEnable(end_time)) {
				log.info("execute failure");
			} else {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date forward_time = sdf.parse(end_time);
				Date afterward_time = new Date(forward_time.getTime() + 180000);
				String begin_time = sdf.format(afterward_time);
				String params[] = { end_time, begin_time };
				String sql = MessageFormat.format(prop.getProperty("sql"), params);

				// 处理前回写记录表
				String nowtime = sdf.format(new Date());
				BussinessUtil.firstWriteBack(meta_conn, table, nowtime, end_time);

				Connection dbconn = null;
				try {
					dbconn = initConn(prop);
					KafkaProducer<String, String> producer = KafkaConnUtil.createProducer(kafka_url);
					extractData(dbconn, sql, fields, splitFlag, producer, topic);
					producer.close();
					dbconn.close();

					// 处理后回写记录表
					nowtime = sdf.format(new Date());
					BussinessUtil.successWriteBack(meta_conn, table, nowtime, end_time);
				} catch (Exception e) {
					log.error("deal error", e);
				}
			}
		} catch (Exception e) {
			log.error("connect meta db error", e);
		} finally {
			if (meta_conn != null) {
				try {
					meta_conn.close();
				} catch (SQLException e) {
					log.error("close meta db error", e);
				}
			}
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

	private static Connection initMateConn(PropertiesUtil prop) throws Exception {
		Connection dbconn = null;
		String driver = prop.getProperty("meta_driver");
		String url = prop.getProperty("meta_url");
		String username = prop.getProperty("meta_username");
		String password = prop.getProperty("meta_password");

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
