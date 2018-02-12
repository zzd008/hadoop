package cn.jxust.bigdata.mr.rjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class InfoBean implements Writable{
	private String id;
	private String date;
	private String pid;
	private int amount;
	private String name;
	private String category_id;
	private int price;
	private int flag;//判断是处理商品数据还是订单数据
	
	public int getFlag() {
		return flag;
	}
	public void setFlag(int flag) {
		this.flag = flag;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getPid() {
		return pid;
	}
	public void setPid(String pid) {
		this.pid = pid;
	}
	public int getAmount() {
		return amount;
	}
	public void setAmount(int amount) {
		this.amount = amount;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCategory_id() {
		return category_id;
	}
	public void setCategory_id(String category_id) {
		this.category_id = category_id;
	}
	public int getPrice() {
		return price;
	}
	public void setPrice(int price) {
		this.price = price;
	}
	public void set(String id, String date, String pid, int amount, String name, String category_id, int price,int flag) {
		this.id = id;
		this.date = date;
		this.pid = pid;
		this.amount = amount;
		this.name = name;
		this.category_id = category_id;
		this.price = price;
		this.flag=flag;
	}
	public InfoBean() {}
	
	@Override
	public String toString() {
		return "id=" + id + ", date=" + date + ", pid=" + pid + ", amount=" + amount + ", name=" + name
				+ ", category_id=" + category_id + ", price=" + price ;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);//序列化string类型
		out.writeUTF(date);
		out.writeUTF(pid);
		out.writeInt(amount);
		out.writeUTF(name);
		out.writeUTF(category_id);
		out.writeInt(price);
		out.writeInt(flag);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		id=in.readUTF();//序列化string类型
		date=in.readUTF();
		pid=in.readUTF();
		amount=in.readInt();
		name=in.readUTF();
		category_id=in.readUTF();
		price=in.readInt();
		flag=in.readInt();
	}
	
	
}
