// Sử dụng thread để chạy song song nhiều tiến trình
// hình chữ nhật đại diện cho công việc và ký hiệu hình thoi đại diện cho cấu trúc rẽ nhánh
// dựa trên file log để thực hiện lấy dữ liệu vào file csv,trạng thái er(hợp lệ) hoặc es(thiếuhỏng) thì kết thúc
import java.io.File;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;

public class ScrappingKQXS {
	//31/3/2012
	 FTPClient ftpClient;
	final String[]field= {"ID","Tỉnh","Khu vực","Ngày","Giải","Kết quả sổ","Giá trị vé","Ngày hết hạn"};
//	WarehouseDB warehouseDB;
//	StagingDB stagingDB;
	ControllDB controllDB;
	public ScrappingKQXS(){
//		warehouseDB=WarehouseDB.getInstance();
//		stagingDB=StagingDB.getInstance();
		Configuration.loadConfiguration();
		Configuration.printConfig();
		controllDB=ControllDB.getInstance();
		ftpClient=new FTPClient();
		
	}
	public void writeCSV(List<String[]> listKQXS,String date, String path,String area) {
	
		try {		
			String filePath=path+area+"_" +date+".csv";
			File file = new File(filePath);
			if(!file.getParentFile().exists()) file.getParentFile().mkdirs();
			CSVWriter csv=new CSVWriter(new FileWriter(file));
			csv.writeNext(field);
			csv.writeAll(listKQXS);
			csv.flush();
			csv.close();
            InputStream inputStream = new FileInputStream(file);
            boolean done = ftpClient.storeFile(Configuration.VITUAL_PATH+"/"+file.getName(), inputStream);
            inputStream.close();
            if (done) {
                System.out.println("Upload thành công");
            }
            else {
            	System.out.println("Upload thất bại");
            }
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Không tìm thấy file");
		} 	

	}

	public void scrappingMienBac(String url,String date) throws IOException {
		if(controllDB.checkLog("MB_"+"_"+date+".csv")!=-1) {
			System.out.println("File lỗi hoặc đã lưu vào CSDL");
			return;
		}
		Document doc = null;
		try {	
				LocalDateTime now = LocalDateTime.now();
				Timestamp time = Timestamp.valueOf(now);
				List<String[]> result = new ArrayList<String[]>();
				doc = Jsoup.connect(url+ date + ".html").userAgent("Jsoup client").timeout(20000).get();
				String syntax = "#noidung .box_kqxs .content>table>tbody";
				Elements list = doc.select(syntax);
				String area="MB";
				if(list.size()==0) {
					controllDB.saveLog(time,area+"_"+date+".csv",0);
					return;
				}
				controllDB.saveLog(time,area+"_"+date+".csv",1);
				// thong tin chung
				String titleTable=doc.select("#noidung .box_kqxs>.top>.bkl>.bkr>.bkm>.title").html();
				String province=getProvince(titleTable);				
				//String dateOfWeek = doc.select("#noidung .box_kqxs>.content>table>tbody>tr:eq(0)>td:eq(0)").html();		
				String[] listAward = { "Giải bảy", "Giải sáu", "Giải năm", "Giải tư", "Giải ba",
						"Giải nhì", "Giải nhất", "Giải Đặc Biệt" };
				String[]values= {"40,000","100,000","200,000","400,000","1,000,000","5,000,000","10,000,000","1,000,000,000"};
				for (Element e : list) {
					for (int i = 1; i <= 8; i++) {
						String award = listAward[(listAward.length-i)];
						String value=values[(values.length-i)];
						Elements resultTable = e.select("tr:" + "eq(" + i + ")>td:eq(1)>div");
						for (Element r : resultTable) {
							String numberResult=r.html();
							if(numberResult.isEmpty()) continue;
							//DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
							DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("dd-MM-yyyy");
							LocalDate expireDate = LocalDate.parse(date, formatter2);
							expireDate=expireDate.plusDays(30);
							String[]kqxs= {UUID.randomUUID().toString(),province,area, date, award, numberResult,value,expireDate.format(formatter2)};
							result.add(kqxs);
						}
					}
				}
				writeCSV(result,date, Configuration.PATH,area);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public String getProvince(String titleTable) {
		String [] tokens=titleTable.split("-");
		String title=tokens[2].trim();
		String [] strings=title.split(" ");
		String province=strings[2]+" "+strings[3];
		return province;
	}
	public void scrapping(String url,String date,String area){
		if(controllDB.checkLog(area+"_"+date+".csv")!=-1) {
			System.out.println("File lỗi hoặc đã lưu vào CSDL");
			return;
		}
		Document doc = null;
		try {
				LocalDateTime now = LocalDateTime.now();
				Timestamp time = Timestamp.valueOf(now);
				List<String[]> result = new ArrayList<String[]>();
				doc = Jsoup.connect(url + date + ".html").userAgent("Jsoup client").timeout(20000).get();
				String syntax = "#noidung .box_kqxs:eq(1) .content>table>tbody>tr>td:eq(1)>table>tbody>tr>td";
				Elements list = doc.select(syntax);
				if(list.size()==0) {
					controllDB.saveLog(time,area+"_"+date+".csv",0);
					return;
				}
				// thong tin chung
				controllDB.saveLog(time,area+"_"+date+".csv",1);
//				String dateOfWeek = doc.select("#noidung .box_kqxs:eq(1) .content table tbody tr td .leftcl .thu a").html();
				String[] listAward = { "Giải tám", "Giải bảy", "Giải sáu", "Giải năm", "Giải tư", "Giải ba",
						"Giải nhì", "Giải nhất", "Giải Đặc Biệt" };
				int pivot=list.size()-1;
				if(list.get(list.size()-2).select("table tbody tr:eq(0)>.tinh>a").size()==0) {
					pivot=pivot-1;
				}
				list=doc.select(syntax+":lt("+pivot+")");
				Elements listValueAward=doc.select("#noidung .box_kqxs:eq(1) .content>table>tbody>tr>td:eq(1)>table>tbody>tr>td:eq("+pivot+")");

				for (Element e : list) {
					
					String province = e.select("table tbody tr:eq(0)>.tinh a").html();
					for (int i = 2; i <= 10; i++) {
						String award = listAward[i - 2];
						String value=listValueAward.select("tr:eq("+i+")>td").html();					
						Elements resultTable = e.select("table tbody tr:" + "eq(" + i + ")>td>div");
						for (Element r : resultTable) {
							String numberResult=r.html();
							if(numberResult.isEmpty()) continue;
							//DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
							DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("dd-MM-yyyy");
							LocalDate expireDate = LocalDate.parse(date, formatter2).plusDays(30);
							String[]kqxs= {UUID.randomUUID().toString(),province,area, date, award, numberResult,value,expireDate.format(formatter2)};
							result.add(kqxs);
				
					}
				}
				writeCSV(result,date, Configuration.PATH,area);
				}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public List<String> getListDate(String startDate, String endDate) {
//	String s = "2022-01-01";
//	String e = "2022-03-11";
		
		List<String> result = new ArrayList<String>();
		LocalDate start = LocalDate.parse(startDate);
		LocalDate end = LocalDate.parse(endDate);
		List<LocalDate> totalDates = new ArrayList<>();
		while (!start.isAfter(end)) {
			totalDates.add(start);
			start = start.plusDays(1);
		}
		SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
		for (LocalDate d : totalDates) {
			Date date = java.sql.Date.valueOf(d);
			result.add(format.format(date));
		}
				return result;
	}
	

	public boolean connectFTPServer() {
		try {
			ftpClient.connect(Configuration.FTP_SERVER_ADDRESS, Configuration.FTP_SERVER_PORT_NUMBER);
		    
		     if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
		    	 ftpClient.disconnect();
		    	 System.out.println("Server không phản hồi!");
		     } 
		     else if (!ftpClient.login(Configuration.FTP_USERNAME, Configuration.FTP_PASSWORD)) {
		         System.out.println("Tài khoản hoặc mật khẩu không đúng!");
		     }
		     ftpClient.enterLocalPassiveMode();
		     ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Xảy ra lỗi trong quá trình kết nối");
			return false;
		}
		return true;

	}

	public void disconnectFTPServer() {
		try {
			if (ftpClient.isConnected()) {
				ftpClient.logout();
				ftpClient.disconnect();
				 System.out.println("Đã đăng xuất FTP");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
public  void getData(String date) throws IOException {
	if(Configuration.isExits==0) {
		System.out.println("Không tồn tại file Config");
		return;
	}
//	 if(!connectFTPServer()) return;
	// Lấy danh sách dữ liệu kết quả xổ số miền nam
	scrapping("https://www.minhngoc.net.vn/doi-so-trung/mien-nam/",date,"MN");
	// Do khác định dạng nên định nghĩa phương thức hơi khác với miền trung và miền nam
	// Lấy danh sách dữ liệu kết quả xổ số miền bắc
	scrappingMienBac("https://www.minhngoc.net.vn/doi-so-trung/mien-bac/",date);
	// Lấy danh sách dữ liệu kết quả xổ số miền trung
	scrapping("https://www.minhngoc.net.vn/doi-so-trung/mien-trung/",date,"MT");
//	disconnectFTPServer();
}
public void getMultiDay() {
	long start=System.currentTimeMillis();
	 if(!connectFTPServer()) return;
	if(Configuration.isExits==0) {
		System.out.println("Không tồn tại file Config");
		return;
	}
	List<String> dates=getListDate("2012-01-01", "2013-01-01");
	if(dates.size()==0) {
		System.out.println("Start date have to more than End date !");
		return ;
	}

	for(String date:dates) {
		try {
			System.out.println(date);
			getData(date);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	disconnectFTPServer();
	long end=System.currentTimeMillis();
	System.out.println("That took "+(end-start)/1000+"s");
}
public void setSchedule() {
		Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, Configuration.HOUR);
        calendar.set(Calendar.MINUTE, Configuration.MINUTE);
        calendar.set(Calendar.SECOND, Configuration.SECOND);
        calendar.set(Calendar.MILLISECOND, 0);

        Date dateSchedule = calendar.getTime();
        long period = 24 * 60 * 60 * 1000;

        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
            	try {
            		
            		SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
            		Date date = java.sql.Date.valueOf(LocalDate.now());	
            		
					getData(format.format(date));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
        };

        Timer timer = new Timer();
        timer.schedule(timerTask, dateSchedule, period);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, CsvException, ParseException {
		ScrappingKQXS sm = new ScrappingKQXS();
		sm.getMultiDay();
		//sm.createDateTable();
		
		// Lấy tất cả dữ liệu
		// Chạy bộ lập lịch tự động lấy dữ liệu mỗi 8 giờ tối
//		sm.getMultiDay();
	}
}
