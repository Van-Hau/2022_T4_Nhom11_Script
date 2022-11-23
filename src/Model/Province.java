package Model;

import java.sql.Date;

public class Province {
	private int id;
	private String name;
	private Date dateUpdate;
	private Date dateExpire;
	public Province(int id, String name, Date dateUpdate, Date dateExpire) {
		this.id = id;
		this.name = name;
		this.dateUpdate = dateUpdate;
		this.dateExpire = dateExpire;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Date getDateUpdate() {
		return dateUpdate;
	}
	public void setDateUpdate(Date dateUpdate) {
		this.dateUpdate = dateUpdate;
	}
	public Date getDateExpire() {
		return dateExpire;
	}
	public void setDateExpire(Date dateExpire) {
		this.dateExpire = dateExpire;
	}
	@Override
	public String toString() {
		return "Province [id=" + id + ", name=" + name + ", dateUpdate=" + dateUpdate + ", dateExpire=" + dateExpire
				+ "]";
	}
	
}
