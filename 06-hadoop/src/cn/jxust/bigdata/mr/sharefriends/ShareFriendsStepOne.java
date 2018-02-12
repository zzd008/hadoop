package cn.jxust.bigdata.mr.sharefriends;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

 /*
  * A:B,C,D,F,E,O
	B:A,C,E,K
	C:F,A,D,I
	D:A,E,F,L。。。。。。。
	求出哪些人两两之间有共同好友，及他俩的共同好友都有谁？
	第一个mr map依次出A、B、C。。。都是谁的好友：<B,A><C,A><D,A><F,A><E,A><O,A> reduce：假设输入为<C,A><C,B><C,E>，则构建两两的好友：<A-B,C><A-E,C><B-E,C>
	第二个mr 用上一个mr输出作为输入，假设为<A-B,C><A-E,C><B-E,C>，则输出即可  reduce：做一下拼接即可<A-B:C,D,E....>
	
  */
public class ShareFriendsStepOne {
	
	static class ShareFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text k=new Text();
		Text v=new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String line = value.toString();//A:B,C,D,F,E,O
			String[] person_friends = line.split(":");
			String person=person_friends[0];//A
			for(String friend:person_friends[1].split(",")){//A的好友
				k.set(friend);
				v.set(person);
				context.write(k, v);//<B,A>,<C,A>,<D,A>,<F,A>,<E,A>,<O,A>
			}
		}
	}
	
	static class ShareFriendsStepOneReducer extends Reducer<Text, Text, Text, Text>{
		Text v=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			//<B,A>,<B,C>,<B,F>,<B,E>..... 可以排一下序
			StringBuffer sb=new StringBuffer();
			for(Text t:values){
				sb.append(t.toString()).append(",");//拼接  A,C,F,E
			}
			v.set(sb.toString());
			context.write(key, v);//<c	A,C,F,E>
		}
	}
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		
		Job job=Job.getInstance();
		
		job.setJarByClass(ShareFriendsStepOne.class);
		
		job.setMapperClass(ShareFriendsStepOneMapper.class);
		job.setReducerClass(ShareFriendsStepOneReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("F:/mrdata/sharefriends/input"));
		FileOutputFormat.setOutputPath(job, new Path("F:/mrdata/sharefriends/ouput"));
		
		System.exit(job.waitForCompletion(true)?0:-1);
		
	}
}
