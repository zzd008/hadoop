package cn.jxust.bigdata.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 统计每一个用户（手机号）所耗费的总上行流量、下行流量、总流量
 */
public class FlowCount {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(FlowCount.class);
		
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
	
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//在这里打断点 观察job的提交过程
		System.exit(job.waitForCompletion(true)?0:1);
	
	}
	
	//一定要写成static的
	//因为在main函数中是按照类名调用方法的,所以要将map和reduce内部类声明为静态的.
	
	public static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
//		static FlowBean fb=new FlowBean();//这样设置为静态的，每次set它的成员变量即可，不然上万行时new那么多的对象出来会很浪费
		
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			
			String phoneNumber=fields[0];//电话号
			long upflow=Long.parseLong(fields[fields.length-3]);
			long dflow=Long.parseLong(fields[fields.length-2]);
			
			context.write(new Text(phoneNumber), new FlowBean(upflow, dflow));//输出value为封装了上行流量和下行流量的自定义数据类型FlowBean
		}
	}
	
	public static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context)throws IOException, InterruptedException {
			long sum_upflow=0;//用户的上行流量总和
			long sum_dflow=0;//用户的下行流量总和
			
			for(FlowBean f:values){
				sum_upflow+=f.getUpflow();
				sum_dflow+=f.getDflow();
			}
			
			FlowBean sum_up_d_flow = new FlowBean(sum_upflow,sum_dflow);//用户的总流量
			
//			context.write(key, new Text(sum_upflow+"\t"+sum_dflow)); 只统计上行流量和下行流量总和
			
			context.write(key, sum_up_d_flow);
		}
		
		/*
		 * 当reduce全部处理完成之后会调用cleanup方法
		 */
		@Override
		protected void cleanup(Reducer<Text, FlowBean, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			
		}
	}
}
