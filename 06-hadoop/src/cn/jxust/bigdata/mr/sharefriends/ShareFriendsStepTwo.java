package cn.jxust.bigdata.mr.sharefriends;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ShareFriendsStepTwo {
	static class ShareFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text k=new Text();
		Text v=new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			//A	I,K,C,B,G,F,H,O,D,
			String line = value.toString();
			String[] person_friends = line.split("\t");
			String person=person_friends[0];
			v.set(person);
			String[] friends = person_friends[1].split(",");
			//排序 防止出现A-B B-A情况
			Arrays.sort(friends);//默认从小到大 B,C,D,F,G,,H,I,O
			
			for(int i=0;i<friends.length-2;i++){
				for(int j=i+1;j<friends.length-1;j++){
					k.set(friends[i]+"-"+friends[j]);
					context.write(k, v);//<B-C A>,<B-D A>,<B-F A>....<C-D A>...
				}
			}
		}
	}
	
	static class ShareFriendsStepTwoReducer extends Reducer<Text, Text, Text, Text>{
		Text v=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			//<B-C D>,<B-C G>,<B-C i>.....
			StringBuffer sb=new StringBuffer();
			for(Text t:values){
				sb.append(t.toString()).append(",");
			}
			v.set(sb.toString());
			context.write(key, v);//<B-C D,G,I>
		}
	}
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		
		Job job=Job.getInstance();
		
		job.setJarByClass(ShareFriendsStepTwo.class);
		
		job.setMapperClass(ShareFriendsStepTwoMapper.class);
		job.setReducerClass(ShareFriendsStepTwoReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("F:/mrdata/sharefriends/ouput/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("F:/mrdata/sharefriends/ouput2"));
		
		System.exit(job.waitForCompletion(true)?0:-1);
		
	}
}
