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
 * 订单表和商品表合到一起
order.txt(订单id, 日期, 商品编号, 数量)
	1001	20150710	P0001	2
	1002	20150710	P0001	3
	1002	20150710	P0002	3
	1003	20150710	P0003	3
product.txt(商品编号, 商品名字, 价格, 数量)
	P0001	小米5	1001	2
	P0002	锤子T1	1000	3
	P0003	锤子	1002	4
 * 
 * reduce端join 
 * select  a.id,a.date,b.name,b.category_id,b.price from t_order a join t_product b on a.pid = b.id
 * 将相关联的条件pid作为key，然后在reduce端进行数据拼接
 * 
 * 缺点：join的操作是在reduce阶段完成，reduce端的处理压力太大，map节点的运算负载则很低，资源利用率不高
 * 且在reduce阶段极易产生数据倾斜（加入某种商品p001销量特比好，它的订单特别多，那么9001 hashcode后会本分到一个reduce去，那么这个reduce要处理的数据特别多，这时就会数据倾斜）
 * 解决办法：map端join 分布式缓存distributedCache
 */
public class RJoin {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		//本地运行  F:mrdata\rjoin\input  F:mrdata\rjoin\output
		/*conf.set("mapreduce.framework.name", "local");
		conf.set("fs.defaultFS", "file:///");*/
		
		Job job=Job.getInstance(conf);
		
		//eclipse提交给集群运行
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
			//获取切片信息 InputSplit是一个顶级抽象类 ，用它的实现类FileSplit
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			String fileName = inputSplit.getPath().getName();
			String pid="";
			if(fileName.startsWith("order")){//处理的是订单文件
				pid=fields[2];
				bean.set(fields[0], fields[1], pid, Integer.parseInt(fields[3]), "", "", 0,1);//flag=1表示处理订单信息
				t.set(pid);
				context.write(t, bean);
			}else{//处理的是商品信息文件
				pid=fields[0];
				bean.set("", "", pid, 0, fields[1], fields[2], Integer.parseInt(fields[3]),0);//flag=0表示处理商品信息
				t.set(pid);
				context.write(t, bean);
			}
		}
	}
	
	public static class JoinReduce extends Reducer<Text, InfoBean, InfoBean, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<InfoBean> values,Context context) throws IOException, InterruptedException {
			InfoBean PBean=new InfoBean();//商品bean
			ArrayList<InfoBean> list=new ArrayList<InfoBean>();//存放订单bean
			
			for(InfoBean i:values)	{
//				System.out.println(i);
				if(i.getFlag()==1){//订单bean
//					InfoBean OBean=i; i是一个引用，把这个引用给OBean再放到集合中，这样i每次变化，集合中得到的就是最后一次的i指向的地址
					InfoBean OBean=new InfoBean();
					try {
						BeanUtils.copyProperties(OBean, i);
					} catch (Exception e) {
						e.printStackTrace();
					} 
					list.add(OBean);
				}else{//商品bean
//					PBean=i;//i是一个遍历指针，这样PBean会等于最后一个i，注意！
					try {
						BeanUtils.copyProperties(PBean, i);
					} catch (Exception e) {
						e.printStackTrace();
					} 
				}
			}
			//拼接
			for(InfoBean i:list){
				InfoBean result=i;
				result.setName(PBean.getName());
				result.setCategory_id(PBean.getCategory_id());
				result.setPrice(PBean.getPrice());
				context.write(result, NullWritable.get());//空NullWritable
			}
			
		}
	}
}
