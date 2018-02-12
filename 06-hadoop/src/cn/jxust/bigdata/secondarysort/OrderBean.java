package cn.jxust.bigdata.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/*
 * ����bean
 */
public class OrderBean implements WritableComparable<OrderBean>{
	private String orderId;//����id
	private String pId;//��Ʒid
	private Double amount;//���
	
	public OrderBean() {}
	
	public void set(String orderId, String pId, Double amount) {
		this.orderId = orderId;
		this.pId = pId;
		this.amount = amount;
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getpId() {
		return pId;
	}

	public void setpId(String pId) {
		this.pId = pId;
	}

	public Double getAmount() {
		return amount;
	}

	public void setAmount(Double amount) {
		this.amount = amount;
	}

	@Override
	public String toString() {
		StringBuffer sb=new StringBuffer();
		sb.append(orderId).append(",");
		sb.append(pId).append(",");
		sb.append(amount);
		return sb.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(orderId);
		out.writeUTF(pId);
		out.writeDouble(amount);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		orderId=in.readUTF();
		pId=in.readUTF();
		amount=in.readDouble();
	}

	//���ն���id��С���󣬽��Ӵ�С
	@Override
	public int compareTo(OrderBean o) {
		int cmp = this.orderId.compareTo(o.getOrderId());//String�����Դ��ĵ�compareto���� Ĭ���Ǵ�С����
		if(cmp==0){
			return -(this.amount.compareTo(o.getAmount()));//���Ӵ�С
//			return this.amount-o.getAmount();
		}
		return cmp;
	}
	

}
