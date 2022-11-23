package Config;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
public class Configuration {
	public static String DATABASEDW;
	public static String DATABASEST;
	public static String DATABASECT;
	public static String MYSQLHOST;
	public static String USER;
	public static String PASS;
	public static String PATH;
	public static int HOUR,MINUTE,SECOND;
	public static String FTP;
	public static String SOURCE_NAME;
	public static String CONTACT;
	public static String ID_CONFIG;
	public static String VITUAL_PATH;
	// ftp config
	public static String FTP_SERVER_ADDRESS;
	public static int FTP_SERVER_PORT_NUMBER;
	public static int FTP_TIMEOUT;
	public static int BUFFER_SIZE;
	public static String FTP_USERNAME;
	public  static  String FTP_PASSWORD;
    public static  String DB_DRIVER;
    public static  String URL;
    public static  int DB_MIN_CONNECTIONS;
    public static  int DB_MAX_CONNECTIONS;
    public static  String LOAD_FILE_TO_STAGING;
    public static  String GETS_KQXS;
    public static  String TRUNCATE_KQXS;
    public static  String CHECK_LOG_CONTAIN;
    public static  String GET_LOG_STATUS_1;
    public static  String SAVE_LOG;
    public static  String CHANGE_LOG_TO_2;
    public static  String GET_DATE;
    public static  String SAVE_DATA;
    public static  String GETS_AREA;
    public static  String GET_ALL_AREA;
    public static  String GET_ALL_AWARD;
    public static  String GET_ALL_PROVINCE;
    public static  String GET_AWARD;
    public static  String GET_PROVINCE;
    public static  String IMPORT_PROVINCE_TO_STAGING;
    public static  String IMPORT_PROVINCE_TO_DATAWAREHOUSE;
    public static  String IMPORT_AREA_TO_STAGING;
    public static  String IMPORT_AREA_TO_DATAWAREHOUSE;
    public static  String IMPORT_AWARD_TO_STAGING;
    public static  String IMPORT_AWARD_TO_DATAWAREHOUSE;
    public static  String TRUNCATE_PROVINCE_STAGING;
    public static  String TRUNCATE_AREA_STAGING;
    public static  String TRUNCATE_AWARD_STAGING;
    public static  String INSERT_AWARD;
    public static  String INSERT_AREA;
    public static  String INSERT_PROVINCE;
    FTPClient  ftpClient;
    public static int isExits=1;
    public static void printConfig() {
    	System.out.println("DATABASEDW: "+DATABASEDW);
    	System.out.println("USER: "+USER);
    	System.out.println("PASS: "+PASS);
    	System.out.println("MYSQLHOST "+MYSQLHOST);
    	System.out.println("PATH: "+PATH);
    	System.out.println("HOUR,MINUTE,SECOND: "+HOUR+","+MINUTE+","+SECOND);
    	System.out.println("FTP: "+FTP);
    	System.out.println("SOURCE_NAME: "+SOURCE_NAME);
    	System.out.println("CONTACT: "+CONTACT);
    	System.out.println("ID_CONFIG: "+ID_CONFIG);
    	System.out.println("VITUAL_PATH: "+VITUAL_PATH);
    	System.out.println("FTP_SERVER_ADDRESS: "+FTP_SERVER_ADDRESS);
    	System.out.println("FTP_TIMEOUT: "+FTP_TIMEOUT);
    	System.out.println("BUFFER_SIZE: "+BUFFER_SIZE);
    	System.out.println("FTP_USERNAME: "+FTP_USERNAME);
    	System.out.println("FTP_PASSWORD: "+ FTP_PASSWORD);
    	System.out.println("URL: "+ URL);
    	System.out.println("DB_DRIVER: "+ DB_DRIVER);
    	System.out.println("DB_MIN_CONNECTIONS: "+ DB_MIN_CONNECTIONS);
    	System.out.println("DB_MAX_CONNECTIONS: "+ DB_MAX_CONNECTIONS);
    }
    public static boolean loadConfiguration() {
    	Properties properties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = Configuration.class.getClassLoader()
                    .getResourceAsStream("config.properties");
            if(inputStream==null) System.out.println("Ops, stream is null !");
            // load properties from file
            properties.load(inputStream);
            // get property by name
            DATABASEDW=properties.getProperty("DATABASEDW");
            DATABASEST=properties.getProperty("DATABASEST");
            DATABASECT=properties.getProperty("DATABASECT");
            MYSQLHOST=properties.getProperty("MYSQLHOST");
            USER=properties.getProperty("USER");
            PASS=properties.getProperty("PASS");
            PATH=properties.getProperty("PATH");
            HOUR=Integer.parseInt(properties.getProperty("HOUR"));
            MINUTE=Integer.parseInt(properties.getProperty("MINUTE"));
            SECOND=Integer.parseInt(properties.getProperty("SECOND"));
            FTP=properties.getProperty("FTP");
            SOURCE_NAME=properties.getProperty("SOURCE_NAME");
            CONTACT=properties.getProperty("CONTACT");
            ID_CONFIG=properties.getProperty("ID_CONFIG");
            VITUAL_PATH=properties.getProperty("VITUAL_PATH");
            FTP_SERVER_ADDRESS=properties.getProperty("FTP_SERVER_ADDRESS");
            FTP_SERVER_PORT_NUMBER=Integer.parseInt(properties.getProperty("FTP_SERVER_PORT_NUMBER"));
            FTP_TIMEOUT=Integer.parseInt(properties.getProperty("FTP_TIMEOUT"));
            BUFFER_SIZE=Integer.parseInt(properties.getProperty("BUFFER_SIZE"));
            FTP_USERNAME=properties.getProperty("FTP_USERNAME");
            FTP_PASSWORD=properties.getProperty("FTP_PASSWORD");
            DB_MAX_CONNECTIONS=Integer.parseInt(properties.getProperty("DB_MAX_CONNECTIONS"));
            DB_MIN_CONNECTIONS=Integer.parseInt(properties.getProperty("DB_MIN_CONNECTIONS"));
            URL=properties.getProperty("URL");
            DB_DRIVER=properties.getProperty("DB_DRIVER");
            LOAD_FILE_TO_STAGING=properties.getProperty("LOAD_FILE_TO_STAGING");
            GETS_KQXS=properties.getProperty("GETS_KQXS");
            TRUNCATE_KQXS=properties.getProperty("TRUNCATE_KQXS");
            CHECK_LOG_CONTAIN=properties.getProperty("CHECK_LOG_CONTAIN");
            GET_LOG_STATUS_1=properties.getProperty("GET_LOG_STATUS_1");
            SAVE_LOG=properties.getProperty("SAVE_LOG");
            CHANGE_LOG_TO_2=properties.getProperty("CHANGE_LOG_TO_2");
            GET_DATE=properties.getProperty("GET_DATE");
            SAVE_DATA=properties.getProperty("SAVE_DATA");
            GETS_AREA=properties.getProperty("GETS_AREA");
            GET_AWARD=properties.getProperty("GET_AWARD");
            GET_PROVINCE=properties.getProperty("GET_PROVINCE");
            IMPORT_PROVINCE_TO_STAGING=properties.getProperty("IMPORT_PROVINCE_TO_STAGING");
            IMPORT_PROVINCE_TO_DATAWAREHOUSE=properties.getProperty("IMPORT_PROVINCE_TO_DATAWAREHOUSE");
            IMPORT_AREA_TO_STAGING=properties.getProperty("IMPORT_AREA_TO_STAGING");
            IMPORT_AREA_TO_DATAWAREHOUSE=properties.getProperty("IMPORT_AREA_TO_DATAWAREHOUSE");
            IMPORT_AWARD_TO_STAGING=properties.getProperty("IMPORT_AWARD_TO_STAGING");
            IMPORT_AWARD_TO_DATAWAREHOUSE=properties.getProperty("IMPORT_AWARD_TO_DATAWAREHOUSE");
            TRUNCATE_PROVINCE_STAGING=properties.getProperty("TRUNCATE_PROVINCE_STAGING");
            TRUNCATE_AREA_STAGING=properties.getProperty("TRUNCATE_AREA_STAGING");
            TRUNCATE_AWARD_STAGING=properties.getProperty("TRUNCATE_AWARD_STAGING");
            INSERT_PROVINCE=properties.getProperty("INSERT_PROVINCE");
            INSERT_AREA=properties.getProperty("INSERT_AREA");
            INSERT_AWARD=properties.getProperty("INSERT_AWARD");
            GET_ALL_AREA=properties.getProperty("GET_ALL_AREA");
            GET_ALL_AWARD=properties.getProperty("GET_ALL_AWARD");
            GET_ALL_PROVINCE=properties.getProperty("GET_ALL_PROVINCE");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Lỗi gì đó ở đây");
        } finally {
            // close objects
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    	return true;
    }
    public static Date convertDate(String date) {
    	java.util.Date parsed;
		java.sql.Date sql;
		SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
		try {
			parsed = format.parse(date);
			sql = new java.sql.Date(parsed.getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			sql = new java.sql.Date(0000, 00, 00);
		}
		return sql;
    }
	public static String getID(Connection con) {
		if(!ID_CONFIG.equals("")) return ID_CONFIG;
		try {
			PreparedStatement ps=con.prepareStatement("select ID from config where Source_Name=? AND Source_Local=? AND FTP=? AND user=? AND pass=?");
			ps.setString(1, SOURCE_NAME);
			ps.setString(2, PATH);
			ps.setString(3, FTP_SERVER_ADDRESS);
			ps.setString(4, USER);
			ps.setString(5, PASS);
			ResultSet rs=ps.executeQuery();
			if(rs.next()) {
				ID_CONFIG= rs.getString(1);
				return ID_CONFIG;
			}
			else {
				ps=con.prepareStatement("insert into config(ID,Source_Name,Source_Local,FTP,user,pass) values(?,?,?,?,?,?)");
				String id= UUID.randomUUID().toString();
				ps.setString(1,id);
				ps.setString(2, SOURCE_NAME);
				ps.setString(3, PATH);
				ps.setString(4, FTP_SERVER_ADDRESS);
				ps.setString(5, USER);
				ps.setString(6, PASS);
				int affect=ps.executeUpdate();
				if(affect>0) return id;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "ERR";
	}
	private void disconnectFTPServer() {
        if (ftpClient != null && ftpClient.isConnected()) {
            try {
                ftpClient.logout();
                ftpClient.disconnect();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
	 private void connectFTPServer() {
		 ftpClient = new FTPClient();
	        try {
	            System.out.println("connecting ftp server...");
	            // connect to ftp server
	            ftpClient.setDefaultTimeout(FTP_TIMEOUT);
	            ftpClient.connect(FTP_SERVER_ADDRESS, FTP_SERVER_PORT_NUMBER);
	            // run the passive mode command
	            ftpClient.enterLocalPassiveMode();
	            // check reply code
	            if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
	                disconnectFTPServer();
	                throw new IOException("FTP server not respond!");
	            } else {
	                ftpClient.setSoTimeout(FTP_TIMEOUT);
	                // login ftp server
	                if (!ftpClient.login(FTP_USERNAME, FTP_PASSWORD)) {
	                    throw new IOException("Username or password is incorrect!");
	                }
	                ftpClient.setDataTimeout(FTP_TIMEOUT);
	                System.out.println("connected");
	            }
	        } catch (IOException ex) {
	            System.out.println("Xảy ra lỗi !");
	        }
	    }

}
