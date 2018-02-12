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
 * �����д������ı����ĵ�����ҳ������Ҫ������������
 * ���ļ��еĵ��ʽ����������ĸ����ʣ����ĸ��ļ��г����˼��Σ�
 * ��һ��mr����map�Ե���+�ļ�����Ϊkey��ͳ�Ƴ�ÿ��������ÿ���ļ��г��ֵ��ܴ�����hello--a.txt 3 hello-b.txt 2
 * �ڶ���mr��������һ��mr�ĳ��������Ϊ���룬�ѵ���+�ļ����иmap�˽�������Ϊkey���ļ���+������Ϊvalue���ɣ�  hello a.txt-3  b.txt-2
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
			String fileName = inputSplit.getPath().getName();//��ȡ�ļ���
			
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
			String[] fields = line.split("\t");//reduce1Ĭ����\t�����   [hello--a.txt,3]
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
				v1=v1+t+"\t";//ƴ��
			}
			v.set(v1);
			context.write(key,v);//<hello,a.txt-->3,b.txt-->2>
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		
		//����job1
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
		
		//����job2
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
