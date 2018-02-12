package cn.jxust.bigdata.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * 自定义分组器 如果不分组 <order_001,99.9> <order_001,88.8> <order_001,77.7> 被分到同一个reduce
 * redcue要把相同的key聚合成一组去处理，但是这些key即orderbean对象都不同，所以就不会被聚合，每个都被单独输出了
 * reduce中默认是根据key的值是否相同来聚合的，我们可以自定义聚合方法
 */
public class OrderIdGroupingCompator extends WritableComparator{
	//指定key的class类型（根据key去比较是否相同），并通过反射来构建出一个对象 
	public OrderIdGroupingCompator() {
		super(OrderBean.class,true);
	}
	
	//
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean a1=(OrderBean)a;
		OrderBean b1=(OrderBean)b;
		//比较两个bean时，指定只比较bean中的orderid
		return a1.getOrderId().compareTo(b1.getOrderId());//0相同 去他都不同
	}
}
