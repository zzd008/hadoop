package cn.jxust.bigdata.mr.rjoin;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.jxust.bigdata.mr.wcdemo.WordCountMapper;
import cn.jxust.bigdata.mr.wcdemo.WordCountReducer;

/*
 * ���������Ʒ��ϵ�һ��
order.txt(����id, ����, ��Ʒ���, ����)
	1001	20150710	P0001	2
	1002	20150710	P0001	3
	1002	20150710	P0002	3
	1003	20150710	P0003	3
product.txt(��Ʒ���, ��Ʒ����, �۸�, ����)
	P0001	С��5	1001	2
	P0002	����T1	1000	3
	P0003	����	1002	4
 * 
 * reduce��join 
 * select  a.id,a.date,b.name,b.category_id,b.price from t_order a join t_product b on a.pid = b.id
 * �������������pid��Ϊkey��Ȼ����reduce�˽�������ƴ��
 * 
 * ȱ�㣺join�Ĳ�������reduce�׶���ɣ�reduce�˵Ĵ���ѹ��̫��map�ڵ�����㸺����ܵͣ���Դ�����ʲ���
 * ����reduce�׶μ��ײ���������б������ĳ����Ʒp001�����رȺã����Ķ����ر�࣬��ô9001 hashcode��᱾�ֵ�һ��reduceȥ����ô���reduceҪ����������ر�࣬��ʱ�ͻ�������б��
 * ����취��map��join �ֲ�ʽ����distributedCache
 */
public class RJoin {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		//��������  F:mrdata\rjoin\input  F:mrdata\rjoin\output
		/*conf.set("mapreduce.framework.name", "local");
		conf.set("fs.defaultFS", "file:///");*/
		
		Job job=Job.getInstance(conf);
		
		//eclipse�ύ����Ⱥ����
		job.setJar("C:/Users/zzd/Desktop/mr-jars/rjoin.jar");
		
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);
		
		job.setOutputKeyClass(InfoBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);		
	}
	
	public static class JoinMapper extends Mapper<LongWritable, Text, Text, InfoBean>{
		InfoBean bean=new InfoBean();
		Text t=new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String line=value.toString();
			String[] fields = line.split("\t");
			//��ȡ��Ƭ��Ϣ InputSplit��һ������������ ��������ʵ����FileSplit
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			String fileName = inputSplit.getPath().getName();
			String pid="";
			if(fileName.startsWith("order")){//������Ƕ����ļ�
				pid=fields[2];
				bean.set(fields[0], fields[1], pid, Integer.parseInt(fields[3]), "", "", 0,1);//flag=1��ʾ��������Ϣ
				t.set(pid);
				context.write(t, bean);
			}else{//���������Ʒ��Ϣ�ļ�
				pid=fields[0];
				bean.set("", "", pid, 0, fields[1], fields[2], Integer.parseInt(fields[3]),0);//flag=0��ʾ������Ʒ��Ϣ
				t.set(pid);
				context.write(t, bean);
			}
		}
	}
	
	public static class JoinReduce extends Reducer<Text, InfoBean, InfoBean, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<InfoBean> values,Context context) throws IOException, InterruptedException {
			InfoBean PBean=new InfoBean();//��Ʒbean
			ArrayList<InfoBean> list=new ArrayList<InfoBean>();//��Ŷ���bean
			
			for(InfoBean i:values)	{
//				System.out.println(i);
				if(i.getFlag()==1){//����bean
//					InfoBean OBean=i; i��һ�����ã���������ø�OBean�ٷŵ������У�����iÿ�α仯�������еõ��ľ������һ�ε�iָ��ĵ�ַ
					InfoBean OBean=new InfoBean();
					try {
						BeanUtils.copyProperties(OBean, i);
					} catch (Exception e) {
						e.printStackTrace();
					} 
					list.add(OBean);
				}else{//��Ʒbean
//					PBean=i;//i��һ������ָ�룬����PBean��������һ��i��ע�⣡
					try {
						BeanUtils.copyProperties(PBean, i);
					} catch (Exception e) {
						e.printStackTrace();
					} 
				}
			}
			//ƴ��
			for(InfoBean i:list){
				InfoBean result=i;
				result.setName(PBean.getName());
				result.setCategory_id(PBean.getCategory_id());
				result.setPrice(PBean.getPrice());
				context.write(result, NullWritable.get());//��NullWritable
			}
			
		}
	}
}
