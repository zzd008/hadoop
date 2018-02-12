package cn.jxust.bigdata.mr.sortflowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/*
 * 自定义排序，mr会根据key排列，默认是按照字典顺序升序
 * 按照用户总流量降序排列
 */
public class SortFlowBean implements WritableComparable<SortFlowBean>{
	private long upFlow;
	private long dFlow;
	private long sumFlow;//其实只指定一个字段就行了
	
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

	public SortFlowBean() {}//无参构造，反射时候用
	
	public SortFlowBean(long upFlow, long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow+dFlow;
	}
	
	public void setAll(long upFlow, long dFlow) {//设置所有的值
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow+dFlow;
	}

	
	
	@Override
	public String toString() {
		return sumFlow+"\t"+upFlow+"\t"+dFlow;
	}

	//序列化
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
	}

	//反序列化
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow=in.readLong();
		dFlow=in.readLong();
		sumFlow=in.readLong();
	}

	//根据总流量sumFlow降序
	@Override
	public int compareTo(SortFlowBean o) {
//		return sumFlow>o.getSumFlow()?1:(sumFlow>o.getSumFlow()?0:-1);//升序 大于返回1 是升/正序：从小到大 大于返回-1是降/到序
		return sumFlow>o.getSumFlow()?-1:(sumFlow>o.getSumFlow()?0:1); 
	}

}
