package cn.jxust.bigdata.mr.sortflowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 自定义排序，mr会根据key排列，默认是按照字典顺序升序
 * 按照用户总流量降序排列
 * 程序的输入文件是已经汇总了用户的总流量的
 */
public class SortFlowCount {
	public static class SortFlowCountMapper extends Mapper<LongWritable, Text, SortFlowBean, Text>{
		/*
		 * 这样就只会new出一个对象出来 但是有一个问题：因为只new了一个对象，每次对它set值，是否map的所有输出都是一样的，都是同一个SortFlowBean对象呢？
		 * SortFlowBean s=new SortFlowBean(); ArrayList list=new ArrayList();
		 * s.set(1,2); list.put(s); s.set(10,20); list.put(s); s.set(100,200); list.put(s); 
		 * for(SortFlowBean s1:list){syso(s1));} 
		 * 这样输出的数据都是100,200,300 因为s是一个引用，它指向内存中的地址，我每次都把s放到集合中，集合中放的是引用，我三次对s进行set设置值，但是它的内存地址没有变，每次改变的是内存中存放的数据，它的数据是最后一次set的值
		 * 
		 * 但是mapper中不一样：
		 * 虽然是同一个SortFlowBean对象，但是map端context.write()是将对象序列化，reduce端将其反序列化，会调用SortFlowBean的readFields()和write()方法，会将每次set的数据写出，
		 * 所以map每次写出的对象是不一样的，所以hashcode也不一样
		 */
		SortFlowBean s=new SortFlowBean();
		Text v=new Text();
		
		/*
		 *map处理的是上一个统计程序的结果，已经是各个手机号得总流量信息
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String lines = value.toString();
			String[] fields = lines.split("\t");
			
//			SortFlowBean s=new SortFlowBean(Long.parseLong(fields[1]),Long.parseLong(fields[2])); 这样写效率很低，每一行都要new一个SortFlowBean
			
			/*s.setUpFlow(Long.parseLong(fields[1]));
			s.setUpFlow(Long.parseLong(fields[2]));
			s.setSumFlow(Long.parseLong(fields[1])+Long.parseLong(fields[2]));*/
			
			s.setAll(Long.parseLong(fields[1]), Long.parseLong(fields[2]));//直接在SortFlowBean中写一个setAll方法设置所有字段
			v.set(fields[0]);
			context.write(s, v);
		}
	}
	
	public static class SortFlowCountReducer extends Reducer<SortFlowBean, Text, SortFlowBean, Text>{
		/*
		 * <bean,phonenum> 因为map端输出的SortFlowBean对象都是不一样的，所以不会有相同的key
		 */
		@Override
		protected void reduce(SortFlowBean key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			context.write(key, values.iterator().next());
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance();
		job.setJarByClass(SortFlowCount.class);
//		job.setNumReduceTasks(1); 要想结果全局有序，必须设置reducetask为1，不然会每个reduce输出文件是有序的，但是放在一起就没序了
		job.setMapperClass(SortFlowCountMapper.class);
		job.setReducerClass(SortFlowCountReducer.class);
		job.setMapOutputKeyClass(SortFlowBean.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(SortFlowBean.class);
		
		//如果输出路径存在就删除
		Path outPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outPath);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
