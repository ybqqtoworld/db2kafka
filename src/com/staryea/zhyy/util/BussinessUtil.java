package com.staryea.zhyy.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

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
	public static String getEndTime(Connection meta_conn, String table) throws Exception {
		String end_time = null;

		String sql1 = "select date_format(end_time, '%Y-%m-%d %H:%i:%s') as end_time from ts_control where table_name='"
				+ table + "' and is_finish=1 order by end_time desc limit 1";
		PreparedStatement pstmt1 = (PreparedStatement) meta_conn.prepareStatement(sql1);
		ResultSet rs1 = pstmt1.executeQuery();

		while (rs1.next()) {
			end_time = rs1.getString("end_time");
		}

		if (!CommonUtil.isEnable(end_time)) {
			log.info("can not get the last end_time");
			end_time = null;
		}

		String sql2 = "select * from ts_control where table_name='" + table
				+ "' and end_time=(select (select end_time from ts_control where table_name='" + table
				+ "' and is_finish=1 order by end_time desc limit 1)+INTERVAL 3 MINUTE);";
		PreparedStatement pstmt2 = (PreparedStatement) meta_conn.prepareStatement(sql2);
		ResultSet rs2 = pstmt2.executeQuery();
		if (rs2.next()) {
			log.info("already dealing the last end_time");
			end_time = null;
		}

		rs1.close();
		pstmt1.close();
		rs2.close();
		pstmt2.close();
		return end_time;
	}

	/**
	 * 
	* @Title: extractData  
	* @Description: TODO
	* @param dbconn
	* @param sql
	* @param fields
	* @param splitFlag
	* @param producer
	* @param topic
	* @throws Exception
	* @return void
	 */
	public static int extractData(Connection dbconn, String sql, String[] fields, String splitFlag,
			KafkaProducer<String, String> producer, String topic) throws Exception {
		int count = 0;
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		int len = fields.length;

		pstmt = (PreparedStatement) dbconn.prepareStatement(sql);
		log.info(sql);
		rs = pstmt.executeQuery();
		while (rs.next()) {
			count++;
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
		return count;
	}

	/**
	 * 
	* @Title: firstWriteBack  
	* @Description: TODO
	* @param meta_conn
	* @param table
	* @param start_time
	* @param end_time
	* @param execute_time
	* @throws Exception
	* @return boolean
	 */
	public static boolean firstWriteBack(Connection meta_conn, String table, String start_time, String end_time,
			String execute_time) throws Exception {
		String sql = "insert into ts_control (table_name,start_time,end_time,is_finish,execute_time) value (?,?,?,?,?)";
		log.info("insert new extract record:" + sql);
		PreparedStatement pstmt = (PreparedStatement) meta_conn.prepareStatement(sql);
		pstmt.setString(1, table);
		pstmt.setString(2, start_time);
		pstmt.setString(3, end_time);
		pstmt.setString(4, "0");
		pstmt.setString(5, execute_time);
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
	* @param  finsh_time 完成时间
	* @param count 数据条数
	* @throws Exception
	* @return boolean
	 */
	public static boolean successWriteBack(Connection meta_conn, String table, String start_time, String end_time,
			String finsh_time, int count) throws Exception {
		String sql = "update ts_control set finish_time='" + finsh_time + "',is_finish='1',num=" + count
				+ " where table_name='" + table + "' and start_time='" + start_time + "' and end_time='" + end_time
				+ "'";
		log.info("update success extract record:" + sql);
		PreparedStatement pstmt = (PreparedStatement) meta_conn.prepareStatement(sql);
		boolean res = pstmt.execute();
		pstmt.close();
		return res;
	}

}
