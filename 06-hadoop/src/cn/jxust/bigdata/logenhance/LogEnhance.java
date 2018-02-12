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
 * 现有一些原始日志需要做增强解析处理，流程：
   1、从原始日志文件中读取数据
   2、根据日志中的一个URL字段到外部知识库(可以从数据库中查)中获取信息（比如这个网页是关于什么内容的）增强到原始日志
   3、如果成功增强，则输出到增强结果目录；如果增强失败，则抽取原始数据中URL字段输出到待爬清单目录

 */
public class LogEnhance {
	static class LogEnhanceMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		Text k=new Text();
		NullWritable v=NullWritable.get();
		Map<String,String> ruleMap=new HashMap<String,String>();//存放url的相关信息
		
		//maptask启动时就从数据库中加载规则信息倒ruleMap中
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
			
			// 获取一个计数器用来记录不合法的日志行数, 会有很多计数器，设置计数器的组名, 计数器名称
			//计数器在程序运行完会输出在控制台上
			Counter counter = context.getCounter("illegal", "illegalline");
			
			try {
				String url=fields[26];//有些行没有26个字段，或者第26个字段不是url不是以http开头的，数据可能不规整
				String content=ruleMap.get(url);
				// 判断内容标签是否为空，如果为空，则只输出url到待爬清单；如果有值，则输出到增强日志
				if(content==null){
					k.set(url+"\t"+ "tocrawl" +"\n");//重新爬
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

		// 要控制不同的内容写往不同的目标路径，可以采用自定义outputformat的方法
		job.setOutputFormatClass(LogEnhanceOutPutFormat.class);

		FileInputFormat.setInputPaths(job, new Path("F:/mrdata/weblogenhance/input"));

		// 尽管我们用的是自定义outputformat，但是它是继承制fileoutputformat
		// 在fileoutputformat中，必须输出一个_success文件作为标志，所以在此还需要设置输出path
		//而真实数据卸载了我们自定义的路径下
		FileOutputFormat.setOutputPath(job, new Path("F:/mrdata/weblogenhance/output"));

		// 不需要reducer
		//reducetask在最终输出时才会调用outputformat，但是程序中不指定reducetask，那么maptask就会直接输出调用outputformat，不会进入shuffle阶段，数据按照什么顺序输入，就按照什么顺序输出
		//不指定reducetask会有一个默认的，它什么都不做，直接在maptask输出了
		job.setNumReduceTasks(0);

		job.waitForCompletion(true);
		System.exit(0);
	}
}
