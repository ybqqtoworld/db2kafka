package com.staryea.zhyy.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BussinessUtil {
	private static final Logger log = LoggerFactory.getLogger(BussinessUtil.class);

	public static String getEndTime(Connection meta_conn, String table) throws Exception {
		String end_time = null;

		String sql1 = "select end_time from ts_control where table_name='" + table
				+ "' and is_finish=1 order by end_time desc limit 1";
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

	public static boolean firstWriteBack(Connection meta_conn, String table, String execute_time, String end_time)
			throws Exception {
		String sql = "update ts_control set execute_time='" + execute_time + "' and is_finsh='0' where table_name='"
				+ table + "' and end_time='" + end_time + "'";
		PreparedStatement pstmt = (PreparedStatement) meta_conn.prepareStatement(sql);
		boolean res = pstmt.execute();
		pstmt.close();
		return res;
	}

	public static boolean successWriteBack(Connection meta_conn, String table, String finsh_time, String end_time)
			throws Exception {
		String sql = "update ts_control set finsh_time='" + finsh_time + "' and is_finsh='1' where table_name='" + table
				+ "' and end_time='" + end_time + "'";
		PreparedStatement pstmt = (PreparedStatement) meta_conn.prepareStatement(sql);
		boolean res = pstmt.execute();
		pstmt.close();
		return res;
	}

}
