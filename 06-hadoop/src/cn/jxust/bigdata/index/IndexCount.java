package cn.jxust.bigdata.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 需求：有大量的文本（文档、网页），需要建立搜索索引
 * 将文件中的单词建立索引：哪个单词，在哪个文件中出现了几次：
 * 第一个mr程序，map以单词+文件名作为key，统计出每个单词在每个文件中出现的总次数：hello--a.txt 3 hello-b.txt 2
 * 第二个mr程序，用上一个mr的程序输出作为输入，把单词+文件名切割，map端将单词作为key，文件名+次数作为value即可：  hello a.txt-3  b.txt-2
 */
public class IndexCount {
	
	static class IndexCountMapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
		Text k=new Text();
		IntWritable v=new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(" ");
			
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			String fileName = inputSplit.getPath().getName();//获取文件名
			
			for(String word:words){
				k.set(word+"--"+fileName);
				context.write(k, v); //<hello--a.txt,1>
			}
			
		}
	}
	
	static class IndexCountReducer1 extends Reducer<Text, IntWritable, Text, IntWritable>{
		IntWritable v=new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int count=0;
			for(IntWritable i:values){
				count+=i.get();
			}
			
			v.set(count);
			context.write(key,v);//<hello--a.txt,3>
		}
	}
	
	
	static class IndexCountMapper2 extends Mapper<LongWritable, Text, Text, Text>{
		Text k=new Text();
		Text v=new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");//reduce1默认以\t做间隔   [hello--a.txt,3]
			String[] fields1 = fields[0].split("--"); //[hello,a.txt]
			
			k.set(fields1[0]);
			v.set(fields1[1]+"-->"+fields[1]);
			context.write(k, v);//<hello,a.txt-->3>
		}
	}
	
	static class IndexCountReducer2 extends Reducer<Text, Text, Text, Text>{
		Text v=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			String v1="";
			for(Text t:values){
				v1=v1+t+"\t";//拼接
			}
			v.set(v1);
			context.write(key,v);//<hello,a.txt-->3,b.txt-->2>
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		
		//配置job1
		Job job1=Job.getInstance();
		
		job1.setJarByClass(IndexCount.class);
		
		job1.setMapperClass(IndexCountMapper1.class);
		job1.setReducerClass(IndexCountReducer1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job1, new Path("F:/mrdata/index/input/"));
		FileOutputFormat.setOutputPath(job1, new Path("F:/mrdata/index/output1"));
		
		job1.waitForCompletion(true);
		
		//配置job2
		Job job2=Job.getInstance();
		
		job2.setJarByClass(IndexCount.class);
		
		job2.setMapperClass(IndexCountMapper2.class);
		job2.setReducerClass(IndexCountReducer2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job2, new Path("F:/mrdata/index/output1/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path("F:/mrdata/index/output2"));
		
		System.exit(job2.waitForCompletion(true)?0:-1);
	}
}
