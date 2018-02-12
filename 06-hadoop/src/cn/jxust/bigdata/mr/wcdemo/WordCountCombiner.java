package cn.jxust.bigdata.mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * (a,1) (a,1)=>(a,2)
 * combiner会提高效率，但是combiner要合理使用，程序调用几次combiner是不确定的
 * 
 * 输入为map的输出
 * 可以不写 因为逻辑和WordCountReducer一样
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int count=0;
		for(IntWritable i:values){
			count+=i.get();
		}
		context.write(key, new IntWritable(count));
	}
}
