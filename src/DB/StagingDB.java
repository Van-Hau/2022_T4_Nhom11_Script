package DB;

import java.beans.PropertyVetoException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import Config.Configuration;
import Model.KQXS;

public class StagingDB {
	private Connection con;
	private static StagingDB instance;
	private static ComboPooledDataSource cpds = new ComboPooledDataSource();
	private StagingDB(){	
//		try {
		//	Configuration.loadConfiguration();
//			Class.forName("com.mysql.cj.jdbc.Driver");	
//			String url= "jdbc:mysql://"+Configuration.MYSQLHOST+"/"+Configuration.DATABASEST;
//			con=DriverManager.getConnection(url,Configuration.USER, Configuration.PASS);
//			} catch (ClassNotFoundException | SQLException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		 try {
	        	Configuration.loadConfiguration();
	            cpds.setDriverClass(Configuration.DB_DRIVER);
	            cpds.setJdbcUrl(Configuration.URL+Configuration.MYSQLHOST+"/"+Configuration.DATABASEST);
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
	public static StagingDB getInstance() {
		if(instance==null) instance=new StagingDB();
		return instance;
	}
	public boolean saveToStagingHelper(String csvFile, int idDate)  {
		System.out.println(idDate);
		PreparedStatement ps;
		try {
			ps = con.prepareStatement(Configuration.LOAD_FILE_TO_STAGING);
			ps.setString(1, Configuration.PATH + csvFile);
			System.out.println(Configuration.PATH + csvFile);
			
			int affect=ps.executeUpdate();
			System.out.println(affect);
			ps.close();
			if(affect>=0) return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return false;
		}
		return false;
		
		
		
		

	}
	public void getLocalInfile() {
		try {
			PreparedStatement ps=con.prepareStatement("show global variables like 'local_infile'");
			ResultSet rs=ps.executeQuery();
			rs.next();
			System.out.println("Local Infile "+rs.getString(2));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public List<KQXS> getAllFromStaging() throws SQLException {
		List<KQXS> result=new ArrayList<KQXS>();
		PreparedStatement psStaging = con.prepareStatement(Configuration.GETS_KQXS);
		ResultSet rs = psStaging.executeQuery();
		while (rs.next()) {
			String province=rs.getString(2);
			String area=rs.getString(3);
			String date=rs.getString(4);
			String award=rs.getString(5);
			String numberResult=rs.getString(6);
			String value=rs.getString(7);
			String dateExpire=rs.getString(8);
			KQXS kqxs=new KQXS(UUID.randomUUID().toString(), province, area, dateExpire, award, numberResult, value,dateExpire);
			result.add(kqxs);
			}
		psStaging.close();
		
		return result;

	}
	public void truncateStaging() throws SQLException {
		PreparedStatement ps = con.prepareStatement(Configuration.TRUNCATE_KQXS);
		ps.executeUpdate();
		ps.close();

	}

}
