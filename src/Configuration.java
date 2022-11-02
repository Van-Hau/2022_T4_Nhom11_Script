
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
public class Configuration {
	static String DATABASEDW;
	static String DATABASEST;
	static String DATABASECT;
	static String USER;
	static String PASS;
	static String PATH;
	static int HOUR,MINUTE,SECOND;
	static String FTP;
	static String SOURCE_NAME;
	static String CONTACT;
	public static String ID_CONFIG;
	static String VITUAL_PATH;
	// ftp config
	static String FTP_SERVER_ADDRESS;
    static int FTP_SERVER_PORT_NUMBER;
    static int FTP_TIMEOUT;
    static int BUFFER_SIZE;
    static String FTP_USERNAME;
    static  String FTP_PASSWORD;
    FTPClient  ftpClient;
    static int isExits=1;
    public static void printConfig() {
    	System.out.println("DATABASEDW: "+DATABASEDW);
    	System.out.println("USER: "+USER);
    	System.out.println("PASS: "+PASS);
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
	static String getID(Connection con) {
		if(!ID_CONFIG.equals("")) return ID_CONFIG;
		try {
			PreparedStatement ps=con.prepareStatement("select ID from config where Source_Name=? AND Source_Local=? AND FTP=? AND user=? AND pass=?");
			ps.setString(1, SOURCE_NAME);
			ps.setString(2, PATH);
			ps.setString(3, FTP);
			ps.setString(4, USER);
			ps.setString(5, PASS);
			ResultSet rs=ps.executeQuery();
			if(rs.next()) {
				System.out.println("da co");
				ID_CONFIG= rs.getString(1);
				return ID_CONFIG;
			}
			else {
				ps=con.prepareStatement("insert into config(ID,Source_Name,Source_Local,FTP,user,pass) values(?,?,?,?,?,?)");
				String id= UUID.randomUUID().toString();
				ps.setString(1,id);
				ps.setString(2, SOURCE_NAME);
				ps.setString(3, PATH);
				ps.setString(4, FTP);
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
