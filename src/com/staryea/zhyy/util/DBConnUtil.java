package com.staryea.zhyy.util;

import java.sql.Connection;
import java.sql.DriverManager;

public class DBConnUtil {

	public static Connection dbconn;

	public static Connection getConn(String driver, String url, String username, String password) throws Exception {
		Connection conn = null;
		Class.forName(driver);
		conn = DriverManager.getConnection(url, username, password);
		return conn;
	}

}
