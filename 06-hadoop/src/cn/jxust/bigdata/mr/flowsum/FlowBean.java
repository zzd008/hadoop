package cn.jxust.bigdata.mr.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/*
 * �Զ����������ͣ�Ҫʵ��WritableComparable�ӿڣ��������л��ͱȽϴ�С 
 * ��Ϊflowbean��Ϊ�����value������Ҫ�Ƚϴ�С����������ֻʵ��writable�ӿڼ���
 * ���Ҫ�ȴ�С��implements WritableComparable<FlowBean> Ȼ����дcompareTo()����
 * WritableComparable�ӿ� ʵ����Writable, Comparable
 */
public class FlowBean implements Writable{
	private long upflow;//��������
	private long dflow;//��������
	private long sumflow;//������
	
	//�����л�ʱ��������Ҫ�����޲ι���
	public FlowBean(){}
	
	public FlowBean(long upflow, long dflow) {
		this.upflow = upflow;
		this.dflow = dflow;
		this.sumflow=upflow+dflow;
	}
	
	public long getUpflow() {
		return upflow;
	}
	public void setUpflow(long upflow) {
		this.upflow = upflow;
	}
	public long getDflow() {
		return dflow;
	}
	public void setDflow(long dflow) {
		this.dflow = dflow;
	}
	public long getSumflow() {
		return sumflow;
	}
	public void setSumflow(long sumflow) {
		this.sumflow = sumflow;
	}
	
	//����д��hdfsʱҪ����tostring����
	@Override
	public String toString() {
		return upflow+"\t"+dflow+"\t"+sumflow;
	}

	/*
	 *���л�����
	 */
	@Override
	public void write(DataOutput out) throws IOException {//ֻҪ���������л�д������
		out.writeLong(upflow);//��ʵ��д���ֽ�
		out.writeLong(dflow);
		out.writeLong(sumflow);
	}

	/*
	 * �����л�����
	 * �����л������л���˳��һ��
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		upflow=in.readLong();//��Ϊ���л�����upflow��д�����У����Զ���ʱ���ȴ����ж�����
		dflow=in.readLong();
		sumflow=in.readLong();
	}

	/*
	 * �Ƚϴ�С�ķ���  ���Ҫ�ȴ�С��implements WritableComparable<FlowBean> Ȼ����д�������
	 * ��Ϊflowbean��Ϊ�����value������Ҫ�Ƚϴ�С
	 */
	/*public int compareTo(FlowBean o) {//a.compareTo(b) 0:a=b -1:a<b 1:a>b 
		long thisupflow = this.upflow;
		long thisdflow = this.dflow;
		long thatupflow = o.getUpflow();
		long thatdflow = o.dflow;
		//�����Զ���ȽϹ��� ���ﰴ������������
		return(thisupflow<thatupflow?-1:(thisupflow==thatupflow?0:1));
	}*/
	
}
