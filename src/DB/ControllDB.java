package DB;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import Config.Configuration;

public class ControllDB {
	private Connection con;
	private static ControllDB instance;
	private static ComboPooledDataSource cpds = new ComboPooledDataSource();
	private ControllDB(){
		try {
        	Configuration.loadConfiguration();
            cpds.setDriverClass(Configuration.DB_DRIVER);
            cpds.setJdbcUrl(Configuration.URL+Configuration.MYSQLHOST+"/"+Configuration.DATABASECT);
            cpds.setUser(Configuration.USER);
            cpds.setPassword(Configuration.PASS);
            cpds.setMinPoolSize(Configuration.DB_MIN_CONNECTIONS);
            cpds.setInitialPoolSize(Configuration.DB_MIN_CONNECTIONS);
            cpds.setMaxPoolSize(Configuration.DB_MAX_CONNECTIONS);
            try {
				con=cpds.getConnection();
			
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("Lỗi kết nối");
			}
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        }
	}
	public static ControllDB getInstance() {
		if(instance==null) instance=new ControllDB();
		return instance;
	}
	public int checkLog(String fileName) {
		try {
			PreparedStatement ps=con.prepareStatement(Configuration.CHECK_LOG_CONTAIN);
			ps.setString(1, Configuration.getID(con));
			ps.setString(2, fileName);
			ResultSet rs=ps.executeQuery();
			if(rs.next()) {
				return rs.getInt("Status");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 return -1;
	}
	
	public List<String> getListFileNameFromLog(){
		List<String> result=new ArrayList<String>();
		try {

			PreparedStatement ps = con.prepareStatement(Configuration.GET_LOG_STATUS_1);
			ps.setInt(1, 1);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String id = rs.getString(1);
				String fileName = rs.getString(2);
				result.add(id+"_"+fileName);
				
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	public String saveLog(Timestamp time, String fileName,int status) {
		String id="";
		try {
			if(checkLog(fileName)!=-1) return id;
			id=UUID.randomUUID().toString();
			PreparedStatement ps=con.prepareStatement(Configuration.SAVE_LOG);
			ps.setString(1,id);
			ps.setString(2, Configuration.getID(con));
			ps.setString(3, fileName);
			ps.setTimestamp(4, time);
			ps.setInt(5, status);
			ps.setString(6, Configuration.CONTACT);
			ps.executeUpdate();
			ps.close();
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return id;
	}
	public boolean changSaveStatus(String id,int status) {
		try {
			PreparedStatement ps = con.prepareStatement(Configuration.CHANGE_LOG_TO_2);
			ps.setInt(1, status);
			ps.setString(2, id);
			int affect = ps.executeUpdate();
			return affect > 0 ? true : false;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

}
