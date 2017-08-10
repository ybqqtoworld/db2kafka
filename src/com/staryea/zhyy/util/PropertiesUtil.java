package com.staryea.zhyy.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtil {
	private static final Logger log = LoggerFactory.getLogger(PropertiesUtil.class);

	private static PropertiesUtil properties = null;
	private Map<String, String> propMap = new HashMap<String, String>();

	public static PropertiesUtil getInstance(String path) {
		if (properties == null) {
			properties = new PropertiesUtil(path);
		}
		return properties;
	}

	PropertiesUtil() {

	}

	@SuppressWarnings("rawtypes")
	private PropertiesUtil(String path) {
		Properties prop = new Properties();
		InputStream in;
		try {
			in = new FileInputStream(path);
			prop.load(in);
			Enumeration en = prop.keys();
			while (en.hasMoreElements()) {
				String name = en.nextElement().toString();
				String value = prop.getProperty(name);
				log.info("name:" + name + ",value:" + value);
				propMap.put(name, value);
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getProperty(String key) {
		return propMap.get(key);
	}

}
