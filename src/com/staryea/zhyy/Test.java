package com.staryea.zhyy;

import java.text.MessageFormat;

public class Test {

	public static void main(String[] args) {
		String sql = "select ORDER_ID,to_char(PAYMENT_ID) as PAYMENT_ID,to_char(ACCT_ID) as ACCT_ID,to_char(SERV_ID) as SERV_ID,ACTION,STATE,to_char(CREATED_DATE,'yyyymmddhh24miss') as CREATED_DATE,to_char(STATE_DATE,'yyyymmddhh24miss') as STATE_DATE,PROCESS_RESULT,SERVICE_TYPE,PROCESS_TYPE,STAFF_ID,BILLING_MODE_ID from ACCT_AB.a_real_time_service_log where CREATED_DATE>=to_date({0},''yyyy-mm-dd hh24:mi:ss'') and CREATED_DATE<to_date({1},''yyyy-mm-dd hh24:mi:ss'') ";
		String params[] = { "'" + "abc" + "'", "'" + "def" + "'" };
		String sql2 = MessageFormat.format(sql, params);
		System.out.println(sql2);
	}

}
