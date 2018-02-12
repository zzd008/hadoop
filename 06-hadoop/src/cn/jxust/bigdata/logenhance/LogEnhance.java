package cn.jxust.bigdata.logenhance;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * ����һЩԭʼ��־��Ҫ����ǿ�����������̣�
   1����ԭʼ��־�ļ��ж�ȡ����
   2��������־�е�һ��URL�ֶε��ⲿ֪ʶ��(���Դ����ݿ��в�)�л�ȡ��Ϣ�����������ҳ�ǹ���ʲô���ݵģ���ǿ��ԭʼ��־
   3������ɹ���ǿ�����������ǿ���Ŀ¼�������ǿʧ�ܣ����ȡԭʼ������URL�ֶ�����������嵥Ŀ¼

 */
public class LogEnhance {
	static class LogEnhanceMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		Text k=new Text();
		NullWritable v=NullWritable.get();
		Map<String,String> ruleMap=new HashMap<String,String>();//���url�������Ϣ
		
		//maptask����ʱ�ʹ����ݿ��м��ع�����Ϣ��ruleMap��
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			try {
				DBLoader.dbLoader(ruleMap);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line, "\t");
			
			// ��ȡһ��������������¼���Ϸ�����־����, ���кܶ�����������ü�����������, ����������
			//�������ڳ��������������ڿ���̨��
			Counter counter = context.getCounter("illegal", "illegalline");
			
			try {
				String url=fields[26];//��Щ��û��26���ֶΣ����ߵ�26���ֶβ���url������http��ͷ�ģ����ݿ��ܲ�����
				String content=ruleMap.get(url);
				// �ж����ݱ�ǩ�Ƿ�Ϊ�գ����Ϊ�գ���ֻ���url�������嵥�������ֵ�����������ǿ��־
				if(content==null){
					k.set(url+"\t"+ "tocrawl" +"\n");//������
					context.write(k, v);
				}else{
					k.set(line+"\t"+content+"\n");
					context.write(k, v);
				}
			} catch (Exception e) {
				counter.increment(1);//+1
			}
			
		}
	}
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(LogEnhance.class);

		job.setMapperClass(LogEnhanceMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// Ҫ���Ʋ�ͬ������д����ͬ��Ŀ��·�������Բ����Զ���outputformat�ķ���
		job.setOutputFormatClass(LogEnhanceOutPutFormat.class);

		FileInputFormat.setInputPaths(job, new Path("F:/mrdata/weblogenhance/input"));

		// ���������õ����Զ���outputformat���������Ǽ̳���fileoutputformat
		// ��fileoutputformat�У��������һ��_success�ļ���Ϊ��־�������ڴ˻���Ҫ�������path
		//����ʵ����ж���������Զ����·����
		FileOutputFormat.setOutputPath(job, new Path("F:/mrdata/weblogenhance/output"));

		// ����Ҫreducer
		//reducetask���������ʱ�Ż����outputformat�����ǳ����в�ָ��reducetask����ômaptask�ͻ�ֱ���������outputformat���������shuffle�׶Σ����ݰ���ʲô˳�����룬�Ͱ���ʲô˳�����
		//��ָ��reducetask����һ��Ĭ�ϵģ���ʲô��������ֱ����maptask�����
		job.setNumReduceTasks(0);

		job.waitForCompletion(true);
		System.exit(0);
	}
}
