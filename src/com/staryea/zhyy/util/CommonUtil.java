package com.staryea.zhyy.util;

public class CommonUtil {
	public static String isNullOrEmpty(String str) {
		if (str == null || "".equals(str.trim()) || str.trim().length() == 0) {
			return "";
		} else {
			return str;
		}
	}

	public static boolean isEnable(String str) {
		if (str == null || "".equals(str.trim()) || str.trim().length() == 0) {
			return false;
		} else {
			return true;
		}
	}

}
