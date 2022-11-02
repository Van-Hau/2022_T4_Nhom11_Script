import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;
import org.apache.commons.net.ftp.FTPReply;

import com.opencsv.exceptions.CsvException;

public class ImportFile {
	Connection con;
	FTPClient ftpClient;
	WarehouseDB warehouseDB;
	StagingDB stagingDB;
	ControllDB controllDB;
	public ImportFile() {
		Configuration.loadConfiguration();
	warehouseDB=WarehouseDB.getInstance();
	stagingDB=StagingDB.getInstance();
	controllDB=ControllDB.getInstance();
	ftpClient = new FTPClient();
	
	}
	public void downloadAllFile() {
		FTPFile[] listFiles = getListFileFromFTPServer(Configuration.VITUAL_PATH, "csv");
		// download list file from ftp server and save to "E:/ftpclient"
		for (FTPFile ftpFile : listFiles) {
			downloadFTPFile(Configuration.VITUAL_PATH + "/" + ftpFile.getName(), Configuration.PATH + ftpFile.getName());
		}
	}

	public void downloadFTPFile(String ftpFilePath, String downloadFilePath) {
		connectFTPServer();
		System.out.println("File " + ftpFilePath + " đang tải xuống...");
		OutputStream outputStream = null;
		boolean success = false;
		try {
			File downloadFile = new File(downloadFilePath);
			outputStream = new BufferedOutputStream(new FileOutputStream(downloadFile));
			// download file from FTP Server
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			ftpClient.setBufferSize(Configuration.BUFFER_SIZE);
			success = ftpClient.retrieveFile(ftpFilePath, outputStream);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				outputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (success) {
			System.out.println("File " + ftpFilePath + " được tải xuống hoàn tất.");
		} else {
			System.out.println("File " + ftpFilePath + " không thể tải xuống.");
		}
	}

	public FTPFile[] getListFileFromFTPServer(String path, final String ext) {
		FTPFile[] listFiles = {};
		// connect ftp server
		connectFTPServer();
		try {

			// list file ends with "jar"
			FTPFile[] ftpFiles = ftpClient.listFiles(path, new FTPFileFilter() {
				public boolean accept(FTPFile file) {
					return file.getName().endsWith(ext);
				}
			});
			if (ftpFiles.length > 0) {
				System.out.println("f");
				return ftpFiles;
			}

		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return listFiles;
	}

	public void connectFTPServer() {
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
			System.out.println("Xảy ra lỗi trong quá trình kết nối");
		}

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

	public void loadToStaging() {
		List<String> listFile=controllDB.getListFileNameFromLog();
		for(String fileName:listFile) {
			String date = fileName.split("_")[2];
			String idLog= fileName.split("_")[0];
			String fileNameReal= fileName.replace(idLog+"_","");
			System.out.println("Ngày " + date);
			System.out.println("FILE NAME: "+fileName);
			downloadFTPFile(Configuration.VITUAL_PATH + "/" + fileNameReal, Configuration.PATH + fileNameReal);
			Date parsed;
			java.sql.Date sql;
			SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
			try {
				parsed = format.parse(date);
				sql = new java.sql.Date(parsed.getTime());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				sql = new java.sql.Date(0000, 00, 00);
			}
			int idDate = warehouseDB.getIdDate(sql);
			try {
				if(stagingDB.saveToStagingHelper(fileNameReal, idDate)) {
					if(warehouseDB.saveToDatabase(idDate,stagingDB.getAllFromStaging(idDate))) {
						if(!controllDB.changSaveStatus(idLog))
						System.out.println("Thay đổi trạng thái thất bại");
						stagingDB.truncateStaging();
						
					}
					else {
						System.out.println("Lưu từ staging vào datawarehouse thất bại");
						
					}
				}
				else {
					System.out.println("Load vào staging thấy bại");
					
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		ImportFile i = new ImportFile();
		i.loadToStaging();
		//Configuration.loadConfiguration();
		//Configuration.printConfig();

	}
}
