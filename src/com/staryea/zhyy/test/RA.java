package com.staryea.zhyy.test;

import java.text.MessageFormat;

import com.staryea.zhyy.util.PropertiesUtil;

public class RA {

	public static void main(String[] args) {
		String jdbc_conf = "E:\\workspace\\javawork\\staryea\\db2kafka\\src\\config\\sqlserver\\vw_di_qx_autolist.properties";
		PropertiesUtil prop = PropertiesUtil.getInstance(jdbc_conf);
		
		String params[]={"abc","abc-3"};
		String msg=MessageFormat.format(prop.getProperty("sql"),params);
        System.out.println("替换后的字符串："+msg);
	}

}
