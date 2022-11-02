
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
public class WarehouseDB {
	private Connection con;
	private static WarehouseDB instance;
	private WarehouseDB(){
		try {
			//Configuration.loadConfiguration();
			Class.forName("com.mysql.cj.jdbc.Driver");	
			String url= "jdbc:mysql://localhost:3306/"+Configuration.DATABASEDW;
			con=DriverManager.getConnection(url,Configuration.USER, Configuration.PASS);
			} catch (ClassNotFoundException | SQLException e) {
				// TODO Auto-generated catch block
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
			PreparedStatement ps = con.prepareStatement("select id from date_dim WHERE date=?");
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
					"insert into data(ID,Province,Area,Date,Award,Number_result,value,isDelete,Date_expire) values(?,?,?,?,?,?,?,?,?)");
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
			if (affect > 0) {
				return true;
				
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		System.out.println("Save to DatabaseHelper took " + (end - start) / 1000 + "s");

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
		try {
			PreparedStatement ps = con.prepareStatement("select * from khu_vuc");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				if (rs.getString(2).compareTo(area) == 0) {
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
		try {
			PreparedStatement ps = con.prepareStatement("select * from giai");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				if (rs.getString(2).compareTo(award) == 0) {
					return rs.getInt(1);
				}
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
		try {
			PreparedStatement ps = con.prepareStatement("select * from tinh_thanh");
			ResultSet rs = ps.executeQuery();

			while (rs.next()) {

				if (rs.getString(2).compareTo(tinh) == 0) {
					return rs.getInt(1);
				}
			}
			ps.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Không thể tìm thấy id tỉnh");
		return -1;

	}
	public boolean saveToDatabase(int idDate,List<KQXS> listKQXS) throws SQLException {
		long start = System.currentTimeMillis();
		for(KQXS kqxs:listKQXS) {
			System.out.println(kqxs);
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
				System.out.println("SQL"+sql);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				sql = new java.sql.Date(0000, 00, 00);
			}
			int idDateExpire=getIdDate(sql);
			long end = System.currentTimeMillis();
//			System.out.println("Save to Database took " + (end - start) + "s");
//			System.out.println(province+"-"+kqxs.getProvince());
//			System.out.println(area+"-"+kqxs.getArea());
//			System.out.println(idDate+"-"+kqxs.getDate());
//			System.out.println(award+"-"+kqxs.getAward());
//			System.out.println(numberResult+"-"+kqxs.getNumberResult());
//			System.out.println(value+"-"+kqxs.getValue());
//			System.out.println(idDateExpire+"-"+kqxs.getDateExpire());
			return saveToDatabaseHelper(UUID.randomUUID().toString(),province, area, idDate, award, numberResult, value,idDateExpire);

		}
		return false;

	}
	public void createDateTable() {
		String start="2000-01-01";
		String end="3000-12-30";
		String[] dates= {"Chủ Nhật","Thứ Hai","Thứ Ba","Thứ Tư","Thứ Năm","Thứ Sáu","Thứ Bảy"};
//		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		LocalDate startDate=LocalDate.parse(start);
		LocalDate endDate=LocalDate.parse(end);
		int index=1;
		while(!startDate.isAfter(endDate)) {
			try {
				PreparedStatement ps=con.prepareStatement("insert into date_dim(id,date,date_of_week) values(?,?,?)");
				ps.setInt(1, (index++));
				System.out.println(index);
				java.sql.Date dateSQL=java.sql.Date.valueOf(startDate);
				ps.setDate(2,dateSQL);
				Calendar c = Calendar.getInstance();
				c.setTime(dateSQL);
				int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
				// 1-Chủ nhật, 2-Thứ Hai, 3-Thứ Ba, 4- Thứ 4, ...
				ps.setString(3, dates[dayOfWeek-1]);
				ps.executeUpdate();
				startDate=startDate.plusDays(1);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
			System.out.println("Lỗi SQL");
			e.printStackTrace();
			}
			
		}
	
	}
	public static void main(String[] args) {
//		WarehouseDB j=WarehouseDB.getInstance();
//		j.createGiai();
	}
}
