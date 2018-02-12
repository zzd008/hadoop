package cn.jxust.bigdata.mr.wcdemo;

import java.io.IOException;
import java.io.Serializable;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * MR��ͨ���ͻ�ָ����inputformat����ȡRecordReader��ͨ��InputSplitȥ�����ݿ�����ȡ���ݣ��γ�����KV�ԣ���Ϊmapper������
 * 
 * KEYIN: Ĭ������£���mr�����������һ���ı�����ʼƫ������Long,
 * 
 * ��Щ����Ҫͨ������ȥ���䣬����Ҫ���л���������ʹ��java�����л��ӿ�Serializable,��ΪSerializable�Ƚ����࣬�������������л����Ὣ��ļ̳нṹҲ���л�
 * ������ֻ��Ҫ���������л����ͼ��ɣ�������hadoop�����Լ��ĸ���������л��ӿڣ����Բ�ֱ����Long������LongWritable
 * 
 * VALUEIN:Ĭ������£���mr�����������һ���ı������ݣ�String��ͬ�ϣ���Text
 * 
 * KEYOUT�����û��Զ����߼��������֮��mapper����������е�key���ڴ˴��ǵ��ʣ�String��ͬ�ϣ���Text
 * VALUEOUT�����û��Զ����߼��������֮��mapper����������е�value���ڴ˴��ǵ��ʴ�����Integer��ͬ�ϣ���IntWritable
 * 
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	/*
	 *map�׶ε�ҵ���߼���д���û��Զ����map����������
	 *maptask���ÿһ���������ݵ���һ�������Զ����map��������
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		/*
		//��ֵ������value���в�֣�Ĭ���Կո�����ֵ Ҳ�����Լ�ָ��
		 StringTokenizer st=new StringTokenizer(value.toString());
		while(st.hasMoreTokens()){
			context.write(new Text(st.nextToken()), new IntWritable(1));
		}*/
		
		
		//��maptask�������ǵ��ı�������ת����String
		String line=value.toString();
		//���ݿո���һ���зֳɵ���
		String[] words = line.split(" ");		
		//���������Ϊ<���ʣ�1>
		for(String word:words){
			//��������Ϊkey��������1��Ϊvalue���Ա��ں��������ݷַ������Ը��ݵ��ʷַ����Ա�����ͬ���ʻᵽ��ͬ��reduce task
			context.write(new Text(word), new IntWritable(1)); // <word,1>����<���ʣ�����>
		}
		
	}
}
