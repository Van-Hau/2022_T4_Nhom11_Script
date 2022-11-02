
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

public class ControllDB {
	private Connection con;
	private static ControllDB instance;
	private ControllDB(){
		try {
			//Configuration.loadConfiguration();
			Class.forName("com.mysql.cj.jdbc.Driver");	
			String url= "jdbc:mysql://localhost:3306/"+Configuration.DATABASECT;
			con=DriverManager.getConnection(url,Configuration.USER, Configuration.PASS);
			} catch (ClassNotFoundException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	public static ControllDB getInstance() {
		if(instance==null) instance=new ControllDB();
		return instance;
	}
	public int checkLog(String fileName) {
		try {
			PreparedStatement ps=con.prepareStatement("select * from log where log.ID_Config=? and log.File_Name=?");
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

			PreparedStatement ps = con.prepareStatement("select ID,File_Name from log where status=?");
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
	public void saveLog(Timestamp time, String fileName,int status) {
		
		try {
			if(checkLog(fileName)!=-1) return ;
			String id=UUID.randomUUID().toString();
			PreparedStatement ps=con.prepareStatement("insert into log(ID, ID_Config, File_Name,Time,Status,Contact) values(?,?,?,?,?,?)");
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
		
	}
	public boolean changSaveStatus(String id) {
		try {
			PreparedStatement ps = con.prepareStatement("update log set Status=? where ID=?");
			ps.setInt(1, 2);
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
