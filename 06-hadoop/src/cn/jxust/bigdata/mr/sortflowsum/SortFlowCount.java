package cn.jxust.bigdata.mr.sortflowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * �Զ�������mr�����key���У�Ĭ���ǰ����ֵ�˳������
 * �����û���������������
 * ����������ļ����Ѿ��������û�����������
 */
public class SortFlowCount {
	public static class SortFlowCountMapper extends Mapper<LongWritable, Text, SortFlowBean, Text>{
		/*
		 * ������ֻ��new��һ��������� ������һ�����⣺��Ϊֻnew��һ������ÿ�ζ���setֵ���Ƿ�map�������������һ���ģ�����ͬһ��SortFlowBean�����أ�
		 * SortFlowBean s=new SortFlowBean(); ArrayList list=new ArrayList();
		 * s.set(1,2); list.put(s); s.set(10,20); list.put(s); s.set(100,200); list.put(s); 
		 * for(SortFlowBean s1:list){syso(s1));} 
		 * ������������ݶ���100,200,300 ��Ϊs��һ�����ã���ָ���ڴ��еĵ�ַ����ÿ�ζ���s�ŵ������У������зŵ������ã������ζ�s����set����ֵ�����������ڴ��ַû�б䣬ÿ�θı�����ڴ��д�ŵ����ݣ��������������һ��set��ֵ
		 * 
		 * ����mapper�в�һ����
		 * ��Ȼ��ͬһ��SortFlowBean���󣬵���map��context.write()�ǽ��������л���reduce�˽��䷴���л��������SortFlowBean��readFields()��write()�������Ὣÿ��set������д����
		 * ����mapÿ��д���Ķ����ǲ�һ���ģ�����hashcodeҲ��һ��
		 */
		SortFlowBean s=new SortFlowBean();
		Text v=new Text();
		
		/*
		 *map���������һ��ͳ�Ƴ���Ľ�����Ѿ��Ǹ����ֻ��ŵ���������Ϣ
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String lines = value.toString();
			String[] fields = lines.split("\t");
			
//			SortFlowBean s=new SortFlowBean(Long.parseLong(fields[1]),Long.parseLong(fields[2])); ����дЧ�ʺܵͣ�ÿһ�ж�Ҫnewһ��SortFlowBean
			
			/*s.setUpFlow(Long.parseLong(fields[1]));
			s.setUpFlow(Long.parseLong(fields[2]));
			s.setSumFlow(Long.parseLong(fields[1])+Long.parseLong(fields[2]));*/
			
			s.setAll(Long.parseLong(fields[1]), Long.parseLong(fields[2]));//ֱ����SortFlowBean��дһ��setAll�������������ֶ�
			v.set(fields[0]);
			context.write(s, v);
		}
	}
	
	public static class SortFlowCountReducer extends Reducer<SortFlowBean, Text, SortFlowBean, Text>{
		/*
		 * <bean,phonenum> ��Ϊmap�������SortFlowBean�����ǲ�һ���ģ����Բ�������ͬ��key
		 */
		@Override
		protected void reduce(SortFlowBean key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			context.write(key, values.iterator().next());
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance();
		job.setJarByClass(SortFlowCount.class);
//		job.setNumReduceTasks(1); Ҫ����ȫ�����򣬱�������reducetaskΪ1����Ȼ��ÿ��reduce����ļ�������ģ����Ƿ���һ���û����
		job.setMapperClass(SortFlowCountMapper.class);
		job.setReducerClass(SortFlowCountReducer.class);
		job.setMapOutputKeyClass(SortFlowBean.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(SortFlowBean.class);
		
		//������·�����ھ�ɾ��
		Path outPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outPath);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
