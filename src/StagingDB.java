
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class StagingDB {
	private Connection con;
	private static StagingDB instance;
	private StagingDB(){
		try {
		//	Configuration.loadConfiguration();
			Class.forName("com.mysql.cj.jdbc.Driver");	
			String url= "jdbc:mysql://localhost:3306/"+Configuration.DATABASEST;
			con=DriverManager.getConnection(url,Configuration.USER, Configuration.PASS);
			} catch (ClassNotFoundException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	public static StagingDB getInstance() {
		if(instance==null) instance=new StagingDB();
		return instance;
	}
	public boolean saveToStagingHelper(String csvFile, int idDate) throws SQLException {

		PreparedStatement ps = con.prepareStatement("LOAD DATA INFILE ? " + "INTO TABLE ket_qua_xo_so "
				+ "FIELDS TERMINATED BY ',' " + "ENCLOSED BY \'\"\' " + "IGNORE 1 ROWS;");
		ps.setString(1, Configuration.PATH + csvFile);
		int affect=ps.executeUpdate();
		ps.close();
		if(affect>0) return true;
		else return false;
		

	}
	public List<KQXS> getAllFromStaging(int idDate) throws SQLException {
		List<KQXS> result=new ArrayList<KQXS>();
		PreparedStatement psStaging = con.prepareStatement("select * from ket_qua_xo_so");
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
		PreparedStatement ps = con.prepareStatement("truncate table ket_qua_xo_so");
		ps.executeUpdate();
		ps.close();

	}

}
