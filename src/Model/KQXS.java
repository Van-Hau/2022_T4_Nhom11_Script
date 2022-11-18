package Model;

public class KQXS {
	private String id;
	private String province;
	private String area;
	private String date;
	private String award;
	private String numberResult;
	private String value;
	private String dateExpire;
	@Override
	public String toString() {
		return "KQXS [id=" + id + ", province=" + province + ", area=" + area + ", date=" + date + ", award=" + award
				+ ", numberResult=" + numberResult + ", value=" + value + ", dateExpire=" + dateExpire + "]";
	}
	public KQXS(String id, String province, String area, String date, String award,
			String numberResult, String value,String dateExpire) {
		this.id = id;
		this.province = province;
		this.area = area;
		this.date = date;
		this.award = award;
		this.numberResult = numberResult;
		this.value = value;
		this.dateExpire=dateExpire;
	}
	public String getDateExpire() {
		return dateExpire;
	}
	public void setDateExpire(String dateExpire) {
		this.dateExpire = dateExpire;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getProvince() {
		return province;
	}
	public void setProvince(String province) {
		this.province = province;
	}
	public String getArea() {
		return area;
	}
	public void setArea(String area) {
		this.area = area;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getAward() {
		return award;
	}
	public void setAward(String award) {
		this.award = award;
	}
	public String getNumberResult() {
		return numberResult;
	}
	public void setNumberResult(String numberResult) {
		this.numberResult = numberResult;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
}
