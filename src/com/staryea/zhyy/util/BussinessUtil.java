package com.staryea.zhyy.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BussinessUtil {
	private static final Logger log = LoggerFactory.getLogger(BussinessUtil.class);

	/**
	 * 
	* @Title: getEndTime  
	* @Description: TODO
	* @param meta_conn
	* @param table
	* @throws Exception
	* @return String
	 */
	public static String getEndTime(Connection meta_conn, String table, String start_time) throws Exception {
		String end_time = null;
		String is_finish = null;

		String sql = "select is_finish,date_format(end_time, '%Y-%m-%d %H:%i:%s') as end_time from ts_control where table_name='"
				+ table + "' order by end_time desc limit 1";
		PreparedStatement pstmt = (PreparedStatement) meta_conn.prepareStatement(sql);
		ResultSet rs = pstmt.executeQuery();

		if (rs.next()) {
			log.info("get the last record");
			end_time = rs.getString("end_time");
			is_finish = rs.getString("is_finish");
			if (CommonUtil.isEnable(is_finish) && "0".equals(is_finish)) {
				log.info("already deal data!");
				end_time = null;
			}
		} else {
			end_time = start_time;
		}

		rs.close();
		pstmt.close();
		return end_time;
	}

	/**
	 * 
	* @Title: extractData  
	* @Description: TODO
	* @param dbconn
	* @param sql
	* @param fields
	* @param time_field
	* @param splitFlag
	* @param producer
	* @param topic
	* @throws Exception
	* @return Map
	 */
	public static Map<String, String> extractData(Connection dbconn, String sql, String[] fields, String time_field,
			String splitFlag, KafkaProducer<String, String> producer, String topic) throws Exception {
		Map<String, String> map = new HashMap<String, String>();

		int count = 0;
		String last_record_time = "";

		ResultSet rs = null;
		PreparedStatement pstmt = null;
		int len = fields.length;

		pstmt = (PreparedStatement) dbconn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_READ_ONLY);
		log.info(sql);
		rs = pstmt.executeQuery();
		while (rs.next()) {
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < len; i++) {
				sb.append(rs.getObject(fields[i]) + splitFlag);
			}

			if (rs.isLast()) {
				last_record_time = rs.getObject(time_field).toString();
			}

			String msg = sb.substring(0, sb.length() - 1);
			// log.info("topic:" + topic + ",msg:" + msg);
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
			producer.send(record);
			count++;
		}

		rs.close();
		pstmt.close();
		map.put("count", count + "");
		map.put("last_record_time", last_record_time);
		return map;
	}

	/**
	 * 
	* @Title: firstWriteBack  
	* @Description: TODO
	* @param meta_conn
	* @param table
	* @param start_time
	* @param end_time
	* @throws Exception
	* @return boolean
	 */
	public static boolean firstWriteBack(Connection meta_conn, String table, String start_time) throws Exception {
		String sql = "insert into ts_control (table_name,start_time,is_finish) value (?,?,?)";
		log.info("insert new extract record:" + sql);
		PreparedStatement pstmt = (PreparedStatement) meta_conn.prepareStatement(sql);
		pstmt.setString(1, table);
		pstmt.setString(2, start_time);
		pstmt.setString(3, "0");
		boolean res = pstmt.execute();
		pstmt.close();
		return res;
	}

	/**
	 * 
	* @Title: successWriteBack 
	* @Description: TODO (成功处理了回写记录)
	* @param  meta_conn
	* @param  table 抽取数据的表
	* @param  start_time 开始时间
	* @param  end_time 结束时间
	* @param count 数据条数
	* @throws Exception
	* @return boolean
	 */
	public static boolean successWriteBack(Connection meta_conn, String table, String start_time, String end_time,
			int count) throws Exception {
		String sql = "update ts_control set end_time='" + end_time + "',is_finish='1',num=" + count
				+ " where table_name='" + table + "' and start_time='" + start_time + "'";
		log.info("update success extract record:" + sql);
		PreparedStatement pstmt = (PreparedStatement) meta_conn.prepareStatement(sql);
		boolean res = pstmt.execute();
		pstmt.close();
		return res;
	}

}
