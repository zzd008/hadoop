package cn.jxust.bigdata.mr.wcdemo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * KEYIN, VALUEIN 与mapper输出的KEYOUT,VALUEOUT类型对应
 * KEYOUT, VALUEOUT 是自定义reduce逻辑处理结果的输出数据类型
 * KEYOUT是单词
 * VLAUEOUT是总次数
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	/*
	 * 每处理一组数据，就调用一次reduce（）方法
	 * 入参key，是一组相同单词kv对的key,value是迭代器：这些kv对中所有的v（单词的次数）
	 * <hello,1> <hello,1> <hello,1> ==>shuffle后:<hello,<1,1,1>>或<hello,<3>>
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int count=0;
		
		/*Iterator<IntWritable> iterator = values.iterator();
		while(iterator.hasNext()){
			count+=iterator.next().get();
		}*/
		
		for(IntWritable value:values){
			count+=value.get();
		}
		context.write(key, new IntWritable(count));
	}
}
