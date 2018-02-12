package cn.jxust.bigdata.mr.wcdemo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * KEYIN, VALUEIN ��mapper�����KEYOUT,VALUEOUT���Ͷ�Ӧ
 * KEYOUT, VALUEOUT ���Զ���reduce�߼��������������������
 * KEYOUT�ǵ���
 * VLAUEOUT���ܴ���
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	/*
	 * ÿ����һ�����ݣ��͵���һ��reduce��������
	 * ���key����һ����ͬ����kv�Ե�key,value�ǵ���������Щkv�������е�v�����ʵĴ�����
	 * <hello,1> <hello,1> <hello,1> ==>shuffle��:<hello,<1,1,1>>��<hello,<3>>
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
