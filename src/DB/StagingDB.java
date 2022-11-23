package DB;

import java.beans.PropertyVetoException;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import Config.Configuration;
import Model.Area;
import Model.Award;
import Model.KQXS;
import Model.Province;

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
		rs.close();
		return result;

	}
	public Area getArea(String area) {
		//System.out.println("Khu vuc la "+area);
		try {
			PreparedStatement ps = con.prepareStatement(Configuration.GETS_AREA);
			ps.setString(1, area);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
					int id=rs.getInt(1);
					String areaName=rs.getString(2);
					Date dateUpdate=rs.getDate(3);
					Date dateExpire=rs.getDate(4);
					return new Area(id, areaName, dateUpdate, dateExpire);
			}
			ps.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public Award getAward(String award) {
		//System.out.println("Khu vuc la "+area);
		Award awardObject = null;
		try {
			PreparedStatement ps = con.prepareStatement(Configuration.GET_AWARD);
			ps.setString(1, award);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
					int id=rs.getInt(1);
					String name=rs.getString(2);
					Date dateUpdate=rs.getDate(3);
					Date dateExpire=rs.getDate(4);
					awardObject=new Award(id, name, dateUpdate, dateExpire);
			}
			ps.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return awardObject;
	}
	public boolean insertAward(String award,int count) {
		try {
			
			PreparedStatement ps=con.prepareStatement(Configuration.INSERT_AWARD);
			ps.setInt(1, count);
			ps.setString(2, award);
			LocalDate date=LocalDate.now();
			ps.setDate(3,Date.valueOf(date) );
			String dateExpire = "9999-12-31";
			LocalDate localDate = LocalDate.parse(dateExpire);
			ps.setDate(4, Date.valueOf(localDate));
			int affect=ps.executeUpdate();
			ps.close();
			if(affect>0) return true;
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("that bai");
			e.printStackTrace();
			return false;
		}
		return false;
	}
	public boolean insertProvince(String province,int count) {
		try {
			
			PreparedStatement ps=con.prepareStatement(Configuration.INSERT_PROVINCE);
			ps.setInt(1, count);
			ps.setString(2, province);
			LocalDate date=LocalDate.now();
			ps.setDate(3,Date.valueOf(date) );
			String dateExpire = "9999-12-31";
			LocalDate localDate = LocalDate.parse(dateExpire);
			ps.setDate(4, Date.valueOf(localDate));
			int affect=ps.executeUpdate();
			ps.close();
			if(affect>0) return true;
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("that bai");
			e.printStackTrace();
			return false;
		}
		return false;
	}
	public Province getProvince(String province) {
		//System.out.println("Khu vuc la "+area);
		try {
			PreparedStatement ps = con.prepareStatement(Configuration.GET_PROVINCE);
			ps.setString(1, province);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
					int id=rs.getInt(1);
					String provinceName=rs.getString(2);
					Date dateUpdate=rs.getDate(3);
					Date dateExpire=rs.getDate(4);
					ps.close();
					return new Province(id,provinceName, dateUpdate, dateExpire);
			}
			ps.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public int getCountProvince() {
		//System.out.println("Khu vuc la "+area);
		try {
			PreparedStatement ps = con.prepareStatement(Configuration.GET_ALL_PROVINCE);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				
					return rs.getInt(1);
			}
			ps.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	public int getCountAward() {
		//System.out.println("Khu vuc la "+area);
		try {
			PreparedStatement ps = con.prepareStatement(Configuration.GET_ALL_AWARD);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				System.out.println("so dong 2: "+rs.getInt(1));
					return rs.getInt(1);
			}
			ps.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	public int getCountArea() {
		//System.out.println("Khu vuc la "+area);
		try {
			PreparedStatement ps = con.prepareStatement(Configuration.GET_ALL_AREA);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
					return rs.getInt(1);
			}
			ps.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	public int importProvince() {
		try {
			PreparedStatement ps=con.prepareStatement(Configuration.IMPORT_PROVINCE_TO_STAGING);
			int affect=ps.executeUpdate();
			if(affect>=0) return affect;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
		
	}
	public int importArea() {
		try {
			PreparedStatement ps=con.prepareStatement(Configuration.IMPORT_AREA_TO_STAGING);
			int affect=ps.executeUpdate();
			if(affect>=0) return affect;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
		
	}
	public int importAward() {
		try {
			PreparedStatement ps=con.prepareStatement(Configuration.IMPORT_AWARD_TO_STAGING);
			int affect=ps.executeUpdate();
			if(affect>=0) {
				return affect;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
		
	}
	public boolean truncateAward() {
		try {
			PreparedStatement ps=con.prepareStatement(Configuration.TRUNCATE_AWARD_STAGING);
			int affect=ps.executeUpdate();
			if(affect>=0) return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
		
	}
	public boolean truncateArea() {
		try {
			PreparedStatement ps=con.prepareStatement(Configuration.TRUNCATE_AREA_STAGING);
			int affect=ps.executeUpdate();
			if(affect>=0) return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
		
	}
	public boolean truncateProvince() {
		try {
			PreparedStatement ps=con.prepareStatement(Configuration.TRUNCATE_PROVINCE_STAGING);
			int affect=ps.executeUpdate();
			if(affect>=0) return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
		
	}
	
	public void truncateStaging() throws SQLException {
		PreparedStatement ps = con.prepareStatement(Configuration.TRUNCATE_KQXS);
		ps.executeUpdate();
		ps.close();

	}

}
