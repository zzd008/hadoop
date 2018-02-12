package cn.jxust.bigdata.secondarysort;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * 	Order_0000001	Pdt_01	222.8
	Order_0000001	Pdt_05	25.8
	Order_0000002	Pdt_03	522.8
	Order_0000002	Pdt_04	122.4
	Order_0000003	Pdt_01	222.8
	现在需要求出每一个订单中成交金额最大的一笔交易
	
	最简单的处理方式：map用订单id作为key，金额作为value。同一个订单记录都会发给同一个reduce，然后在reduce对金额做排序，输出最大的即可。当一个订单中有很多商品的时候，排序会很耗时，这样效率就会不高。
	所以：
	1、利用“订单id和成交金额”作为key，可以将map阶段读取到的所有订单数据按照id分区，按照金额排序，发送到reduce
	2、在reduce端利用groupingcomparator将订单id相同的kv聚合成组，然后取第一个即是最大值
 */
public class SecondarySort {
	
	static class SecondarySortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
		OrderBean bean=new OrderBean();
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line, ",");
			bean.set(fields[0], fields[1], Double.parseDouble(fields[2]));
			context.write(bean, NullWritable.get());
		}
	}
	
	static class SecondarySortReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		
		Job job=Job.getInstance();
		
		job.setJarByClass(SecondarySort.class);
		
		job.setMapperClass(SecondarySortMapper.class);
		job.setReducerClass(SecondarySortReducer.class);
		
		job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(OrderBean.class); 
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		//指定使用哪种分组聚合器
		job.setGroupingComparatorClass(OrderIdGroupingCompator.class);
		
		//指定分区器
		job.setPartitionerClass(OrderIdPartioner.class);
		
		FileInputFormat.setInputPaths(job, new Path("F:/mrdata/secondarysort/input"));
		FileOutputFormat.setOutputPath(job, new Path("F:/mrdata/secondarysort/output"));
	
		System.exit(job.waitForCompletion(true)?0:-1);
	}
}
