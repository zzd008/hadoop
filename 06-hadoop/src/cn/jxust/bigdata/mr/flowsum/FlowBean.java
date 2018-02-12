package cn.jxust.bigdata.mr.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/*
 * 自定义数据类型，要实现WritableComparable接口，用于序列化和比较大小 
 * 因为flowbean作为输出的value，不需要比较大小，所以这里只实现writable接口即可
 * 如果要比大小就implements WritableComparable<FlowBean> 然后重写compareTo()方法
 * WritableComparable接口 实现了Writable, Comparable
 */
public class FlowBean implements Writable{
	private long upflow;//上行流量
	private long dflow;//下行流量
	private long sumflow;//总流量
	
	//反序列化时，反射需要调用无参构造
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
	
	//将它写入hdfs时要调用tostring方法
	@Override
	public String toString() {
		return upflow+"\t"+dflow+"\t"+sumflow;
	}

	/*
	 *序列化方法
	 */
	@Override
	public void write(DataOutput out) throws IOException {//只要将数据序列化写出即可
		out.writeLong(upflow);//其实是写成字节
		out.writeLong(dflow);
		out.writeLong(sumflow);
	}

	/*
	 * 反序列化方法
	 * 反序列化和序列化的顺序一致
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		upflow=in.readLong();//因为序列化是是upflow先写进流中，所以读的时候先从流中读出来
		dflow=in.readLong();
		sumflow=in.readLong();
	}

	/*
	 * 比较大小的方法  如果要比大小就implements WritableComparable<FlowBean> 然后重写这个方法
	 * 因为flowbean作为输出的value，不需要比较大小
	 */
	/*public int compareTo(FlowBean o) {//a.compareTo(b) 0:a=b -1:a<b 1:a>b 
		long thisupflow = this.upflow;
		long thisdflow = this.dflow;
		long thatupflow = o.getUpflow();
		long thatdflow = o.dflow;
		//可以自定义比较规则 这里按照上行流量比
		return(thisupflow<thatupflow?-1:(thisupflow==thatupflow?0:1));
	}*/
	
}
