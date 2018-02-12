package cn.jxust.bigdata.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * 自定义分区，按照订单id分区，相同订单号分给同一个reduce
 * 如果只有一个reducetask，就不用分区了
 */
public class OrderIdPartioner extends Partitioner<OrderBean, NullWritable>{

	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
		//相同id的订单bean，会发往相同的partition
		//而且，产生的分区数，是会跟用户设置的reduce task数保持一致
//		return key.getOrderId().hashCode()%numPartitions;
		return (key.getOrderId().hashCode()&Integer.MAX_VALUE)%numPartitions;
	}
	

}
