package cn.jxust.bigdata.mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * (a,1) (a,1)=>(a,2)
 * combiner�����Ч�ʣ�����combinerҪ����ʹ�ã�������ü���combiner�ǲ�ȷ����
 * 
 * ����Ϊmap�����
 * ���Բ�д ��Ϊ�߼���WordCountReducerһ��
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
