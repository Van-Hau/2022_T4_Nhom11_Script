package DB;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import Config.Configuration;
import Model.KQXS;
public class WarehouseDB {
	private Connection con;
	private static WarehouseDB instance;
	private static ComboPooledDataSource cpds = new ComboPooledDataSource();
	private WarehouseDB(){
		 try {
	        	Configuration.loadConfiguration();
	            cpds.setDriverClass(Configuration.DB_DRIVER);
	            cpds.setJdbcUrl(Configuration.URL+Configuration.MYSQLHOST+"/"+Configuration.DATABASEDW);
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
	public static WarehouseDB getInstance() {
		if(instance==null) instance=new WarehouseDB();
		return instance;
	}
	public int getIdDate(java.sql.Date date) {
		int id = -1;
		try {
			PreparedStatement ps = con.prepareStatement(Configuration.GET_DATE);
			ps.setDate(1, date);
			ResultSet rs = ps.executeQuery();

			if (rs.next())
				id = rs.getInt(1);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return id;
	}
	public boolean saveToDatabaseHelper(String id,int tinh, int khuVuc, int ngay, int giai,
			String ketQuaSo, int giaTri,int dateExpire) {
		long start = System.currentTimeMillis();
		try {
			PreparedStatement ps = con.prepareStatement(
					Configuration.SAVE_DATA);
			ps.setString(1, id);
			ps.setInt(2, tinh);
			ps.setInt(3, khuVuc);
			ps.setInt(4, ngay);
			ps.setInt(5, giai);
			ps.setString(6, ketQuaSo);
			ps.setInt(7, giaTri);
			ps.setInt(8, 0);
			ps.setInt(9, dateExpire);
			int affect = ps.executeUpdate();
			long end = System.currentTimeMillis();
			System.out.println("Save to DatabaseHelper took " + (end - start) + "s");

			if (affect > 0) {
				return true;
				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}
//	public void createGiai() {
//		String[] listAward = { "Giải tám", "Giải bảy", "Giải sáu", "Giải năm", "Giải tư", "Giải ba",
//				"Giải nhì", "Giải nhất", "Giải Đặc Biệt" };
//		try {
//			int stt=0;
//			PreparedStatement ps=con.prepareStatement("insert into giai(id,Name_award,date_update,date_expire) values(?,?,?,?)");
//			for(String j:listAward) {	
//				System.out.println(stt);
//				ps.setInt(1, stt);
//				ps.setString(2, listAward[stt]);
//				ps.setDate(3, java.sql.Date.valueOf("2022-10-25"));
//				ps.setDate(4, java.sql.Date.valueOf("9999-12-31"));
//				ps.executeUpdate();
//				stt++;
//			}
//			ps.close();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	public int getIdArea(String area) {
		//System.out.println("Khu vuc la "+area);
		try {
			PreparedStatement ps = con.prepareStatement(Configuration.GETS_AREA);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				if (rs.getString(2).compareTo(area) == 0) {
					//System.out.println("Khu vuc "+rs.getInt(1));
					return rs.getInt(1);
				}
			}
			ps.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Không thể tìm thấy id khu vực");
		return -1;
	}
	public int getIdAward(String award) {
		//System.out.println("giai la "+award);
		try {
			PreparedStatement ps = con.prepareStatement(Configuration.GET_AWARD);
			ps.setString(1, award);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
					return rs.getInt(1);						
			}
			ps.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Không thể tìm thấy id Giải");
		return -1;
	}
	public int getIdProvince(String tinh) {
		//System.out.println("tinh la "+tinh);
		try {
			PreparedStatement ps = con.prepareStatement(Configuration.GET_PROVINCE);
			ps.setString(1, tinh);
			ResultSet rs = ps.executeQuery();

			if (rs.next()) {
				
				
					return rs.getInt(1);
				
			}
			ps.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Không thể tìm thấy id tỉnh");
		return -1;

	}
	public boolean saveToDatabase(int idDate,List<KQXS> listKQXS){
		long start = System.currentTimeMillis();
		int affect=0;
		System.out.println("staging length "+listKQXS.size());
		for(KQXS kqxs:listKQXS) {
			long start1 = System.currentTimeMillis();
			int province = getIdProvince(kqxs.getProvince());
			if (province == -1) {
				System.out.println("Khong tim thay id cua tinh " + kqxs.getProvince());
				continue;
			}
			int area = getIdArea(kqxs.getArea());
			int award = getIdAward(kqxs.getAward());
			String numberResult = kqxs.getNumberResult();
			int value ;
			try {
				String valueString = kqxs.getValue().replace(",", "");
				value = Integer.parseInt(valueString);
			} catch (NumberFormatException e) {
				// TODO: handle exception
				value=0;
			}
			Date parsed;
			java.sql.Date sql;
			SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
			try {
				parsed = format.parse(kqxs.getDateExpire());
				sql = new java.sql.Date(parsed.getTime());
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				sql = new java.sql.Date(0000, 00, 00);
			}
			int idDateExpire=getIdDate(sql);
			
			if(saveToDatabaseHelper(UUID.randomUUID().toString(),province, area, idDate, award, numberResult, value,idDateExpire)) {
				affect++;
			}
			long end1 = System.currentTimeMillis();
			System.out.println("Handle That took "+(end1-start1)+"s");

		}
		long end = System.currentTimeMillis();
		System.out.println("saveToDatabase That took "+(end-start)/1000+"s");
		if(affect>0) return true;
		else return false;
		

	}
//	public void createDateTable() {
//		String start="2000-01-01";
//		String end="3000-12-30";
//		String[] dates= {"Chủ Nhật","Thứ Hai","Thứ Ba","Thứ Tư","Thứ Năm","Thứ Sáu","Thứ Bảy"};
////		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//		LocalDate startDate=LocalDate.parse(start);
//		LocalDate endDate=LocalDate.parse(end);
//		int index=1;
//		while(!startDate.isAfter(endDate)) {
//			try {
//				PreparedStatement ps=con.prepareStatement("insert into date_dim(id,date,date_of_week) values(?,?,?)");
//				ps.setInt(1, (index++));
//				System.out.println(index);
//				java.sql.Date dateSQL=java.sql.Date.valueOf(startDate);
//				ps.setDate(2,dateSQL);
//				Calendar c = Calendar.getInstance();
//				c.setTime(dateSQL);
//				int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
//				// 1-Chủ nhật, 2-Thứ Hai, 3-Thứ Ba, 4- Thứ 4, ...
//				ps.setString(3, dates[dayOfWeek-1]);
//				ps.executeUpdate();
//				startDate=startDate.plusDays(1);
//			} catch (SQLException e) {
//				// TODO Auto-generated catch block
//			System.out.println("Lỗi SQL");
//			e.printStackTrace();
//			}
//			
//		}
//	
//	}

}
