package cn.jxust.bigdata.mr.wcdemo;

import java.io.IOException;
import java.io.Serializable;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * MR会通过客户指定的inputformat来获取RecordReader，通过InputSplit去切数据块来读取数据，形成输入KV对，作为mapper的输入
 * 
 * KEYIN: 默认情况下，是mr框架所读到的一行文本的起始偏移量，Long,
 * 
 * 这些数据要通过网络去传输，所以要序列化，但不是使用java的序列化接口Serializable,因为Serializable比较冗余，不仅将数据序列化还会将类的继承结构也序列化
 * 而我们只需要将数据序列化后发送即可，所以在hadoop中有自己的更精简的序列化接口，所以不直接用Long，而用LongWritable
 * 
 * VALUEIN:默认情况下，是mr框架所读到的一行文本的内容，String，同上，用Text
 * 
 * KEYOUT：是用户自定义逻辑处理完成之后mapper端输出数据中的key，在此处是单词，String，同上，用Text
 * VALUEOUT：是用户自定义逻辑处理完成之后mapper端输出数据中的value，在此处是单词次数，Integer，同上，用IntWritable
 * 
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	/*
	 *map阶段的业务逻辑就写在用户自定义的map（）方法中
	 *maptask会对每一行输入数据调用一次我们自定义的map（）方法
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		/*
		//分值器　将value进行拆分，默认以空格来分值 也可以自己指定
		 StringTokenizer st=new StringTokenizer(value.toString());
		while(st.hasMoreTokens()){
			context.write(new Text(st.nextToken()), new IntWritable(1));
		}*/
		
		
		//将maptask传给我们的文本内容先转换成String
		String line=value.toString();
		//根据空格将这一行切分成单词
		String[] words = line.split(" ");		
		//将单词输出为<单词，1>
		for(String word:words){
			//将单词作为key，将次数1作为value，以便于后续的数据分发，可以根据单词分发，以便于相同单词会到相同的reduce task
			context.write(new Text(word), new IntWritable(1)); // <word,1>即：<单词，次数>
		}
		
	}
}
