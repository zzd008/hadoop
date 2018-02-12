package cn.jxust.bigdata.mr.sortflowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/*
 * �Զ�������mr�����key���У�Ĭ���ǰ����ֵ�˳������
 * �����û���������������
 */
public class SortFlowBean implements WritableComparable<SortFlowBean>{
	private long upFlow;
	private long dFlow;
	private long sumFlow;//��ʵָֻ��һ���ֶξ�����
	
	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getdFlow() {
		return dFlow;
	}

	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	public SortFlowBean() {}//�޲ι��죬����ʱ����
	
	public SortFlowBean(long upFlow, long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow+dFlow;
	}
	
	public void setAll(long upFlow, long dFlow) {//�������е�ֵ
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow+dFlow;
	}

	
	
	@Override
	public String toString() {
		return sumFlow+"\t"+upFlow+"\t"+dFlow;
	}

	//���л�
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
	}

	//�����л�
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow=in.readLong();
		dFlow=in.readLong();
		sumFlow=in.readLong();
	}

	//����������sumFlow����
	@Override
	public int compareTo(SortFlowBean o) {
//		return sumFlow>o.getSumFlow()?1:(sumFlow>o.getSumFlow()?0:-1);//���� ���ڷ���1 ����/���򣺴�С���� ���ڷ���-1�ǽ�/����
		return sumFlow>o.getSumFlow()?-1:(sumFlow>o.getSumFlow()?0:1); 
	}

}
