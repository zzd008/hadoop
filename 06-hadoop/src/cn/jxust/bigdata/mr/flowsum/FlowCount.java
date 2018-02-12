package cn.jxust.bigdata.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * ͳ��ÿһ���û����ֻ��ţ����ķѵ�����������������������������
 */
public class FlowCount {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(FlowCount.class);
		
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
	
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//�������ϵ� �۲�job���ύ����
		System.exit(job.waitForCompletion(true)?0:1);
	
	}
	
	//һ��Ҫд��static��
	//��Ϊ��main�������ǰ����������÷�����,����Ҫ��map��reduce�ڲ�������Ϊ��̬��.
	
	public static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
//		static FlowBean fb=new FlowBean();//��������Ϊ��̬�ģ�ÿ��set���ĳ�Ա�������ɣ���Ȼ������ʱnew��ô��Ķ����������˷�
		
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			
			String phoneNumber=fields[0];//�绰��
			long upflow=Long.parseLong(fields[fields.length-3]);
			long dflow=Long.parseLong(fields[fields.length-2]);
			
			context.write(new Text(phoneNumber), new FlowBean(upflow, dflow));//���valueΪ��װ�����������������������Զ�����������FlowBean
		}
	}
	
	public static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context)throws IOException, InterruptedException {
			long sum_upflow=0;//�û������������ܺ�
			long sum_dflow=0;//�û������������ܺ�
			
			for(FlowBean f:values){
				sum_upflow+=f.getUpflow();
				sum_dflow+=f.getDflow();
			}
			
			FlowBean sum_up_d_flow = new FlowBean(sum_upflow,sum_dflow);//�û���������
			
//			context.write(key, new Text(sum_upflow+"\t"+sum_dflow)); ֻͳ���������������������ܺ�
			
			context.write(key, sum_up_d_flow);
		}
		
		/*
		 * ��reduceȫ���������֮������cleanup����
		 */
		@Override
		protected void cleanup(Reducer<Text, FlowBean, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			
		}
	}
}
