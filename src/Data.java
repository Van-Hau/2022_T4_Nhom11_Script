
public class Data {
	private String id;
	private int province;
	private int area;
	private int dateOfWeek;
	private int idDate;
	private int award;
	private int numberResult;
	private int value;
	public Data(String id, int province, int area, int dateOfWeek, int idDate, int award, int numberResult, int value) {
		this.id = id;
		this.province = province;
		this.area = area;
		this.dateOfWeek = dateOfWeek;
		this.idDate = idDate;
		this.award = award;
		this.numberResult = numberResult;
		this.value = value;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public int getProvince() {
		return province;
	}
	public void setProvince(int province) {
		this.province = province;
	}
	public int getArea() {
		return area;
	}
	public void setArea(int area) {
		this.area = area;
	}
	public int getDateOfWeek() {
		return dateOfWeek;
	}
	public void setDateOfWeek(int dateOfWeek) {
		this.dateOfWeek = dateOfWeek;
	}
	public int getIdDate() {
		return idDate;
	}
	public void setIdDate(int idDate) {
		this.idDate = idDate;
	}
	public int getAward() {
		return award;
	}
	public void setAward(int award) {
		this.award = award;
	}
	public int getNumberResult() {
		return numberResult;
	}
	public void setNumberResult(int numberResult) {
		this.numberResult = numberResult;
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	
}
