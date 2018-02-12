package cn.jxust.bigdata.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * �Զ�����������ն���id��������ͬ�����ŷָ�ͬһ��reduce
 * ���ֻ��һ��reducetask���Ͳ��÷�����
 */
public class OrderIdPartioner extends Partitioner<OrderBean, NullWritable>{

	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
		//��ͬid�Ķ���bean���ᷢ����ͬ��partition
		//���ң������ķ��������ǻ���û����õ�reduce task������һ��
//		return key.getOrderId().hashCode()%numPartitions;
		return (key.getOrderId().hashCode()&Integer.MAX_VALUE)%numPartitions;
	}
	

}
