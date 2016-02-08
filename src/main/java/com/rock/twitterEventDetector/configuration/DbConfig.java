package com.rock.twitterEventDetector.configuration;

import java.io.IOException;
import java.util.Properties;

public class DbConfig {
	 private static String CONFIG_FILE = "/configs/db.properties";
	  private Properties properties = new Properties();
 	 
	 public DbConfig() {
		 try {
			properties.load(this.getClass().getResourceAsStream(CONFIG_FILE));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	 
	//metodi per configurare il db
		public String getServerName() {
			// TODO Auto-generated method stub
			return this.properties.getProperty("serverName");
		}
		public String getPort() {
			// TODO Auto-generated method stub
			return this.properties.getProperty("port");
		}
		public String getPassword() {
			// TODO Auto-generated method stub
			return this.properties.getProperty("password");
		}
		public String getUserName() {
			// TODO Auto-generated method stub
			return this.properties.getProperty("userName");
		}
		public String getDbName() {
			// TODO Auto-generated method stub
			return this.properties.getProperty("dbName");
		}
		
		public static void main(String[] args) {
			DbConfig dbC=new DbConfig();
			System.out.println(dbC.getDbName());
		}
}
