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
	������Ҫ���ÿһ�������гɽ��������һ�ʽ���
	
	��򵥵Ĵ���ʽ��map�ö���id��Ϊkey�������Ϊvalue��ͬһ��������¼���ᷢ��ͬһ��reduce��Ȼ����reduce�Խ��������������ļ��ɡ���һ���������кܶ���Ʒ��ʱ�������ܺ�ʱ������Ч�ʾͻ᲻�ߡ�
	���ԣ�
	1�����á�����id�ͳɽ�����Ϊkey�����Խ�map�׶ζ�ȡ�������ж������ݰ���id���������ս�����򣬷��͵�reduce
	2����reduce������groupingcomparator������id��ͬ��kv�ۺϳ��飬Ȼ��ȡ��һ���������ֵ
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
		
		//ָ��ʹ�����ַ���ۺ���
		job.setGroupingComparatorClass(OrderIdGroupingCompator.class);
		
		//ָ��������
		job.setPartitionerClass(OrderIdPartioner.class);
		
		FileInputFormat.setInputPaths(job, new Path("F:/mrdata/secondarysort/input"));
		FileOutputFormat.setOutputPath(job, new Path("F:/mrdata/secondarysort/output"));
	
		System.exit(job.waitForCompletion(true)?0:-1);
	}
}
