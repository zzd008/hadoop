package cn.jxust.bigdata.logenhance;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * reducetask在最终输出时才会调用outputformat，但是程序中不指定reducetask，那么maptask就会直接输出调用outputformat，不会进入shuffle阶段，数据按照什么顺序输入，就按照什么顺序输出
 * 
 * 自定义输出模板 因为是输出到文件系统，所以继承FileOutputFormat 还有可以输出到数据库等很多地方
 * maptask或者reducetask在最终输出时，先调用OutputFormat的getRecordWriter方法拿到一个RecordWriter
 * 然后每context.write()一组数据时就会调用一次RecordWriter的write(k,v)方法将数据写出
 * 所以还要自定义一个RecordWriter，继承RecordWriter，重写write方法
 */
public class LogEnhanceOutPutFormat extends FileOutputFormat<Text, NullWritable>{
	
	//获取RecordWriter
	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		FileSystem fs=FileSystem.get(context.getConfiguration());//通过context来获取程序的Configuration配置
		
		//这里输出路径指定为本地  
		Path enhancePath=new Path("F:/mrdata/weblogenhance/enhance/log.dat");//增强日志路径
		Path tocrawlPath=new Path("F:/mrdata/weblogenhance/tocrawl/url.dat");//重新爬虫路径
		
		FSDataOutputStream enhanceOS = fs.create(enhancePath);
		FSDataOutputStream tocrawlOS = fs.create(tocrawlPath);
		
		return new MyRecordWriter(enhanceOS,tocrawlOS);//通过构造函数传到write方法中
	}
	
	static class MyRecordWriter extends RecordWriter<Text, NullWritable>{
		FSDataOutputStream enhanceOS = null;
		FSDataOutputStream tocrawlOS =null;
				
		public MyRecordWriter(FSDataOutputStream enhanceOS, FSDataOutputStream tocrawlOS) {
			this.enhanceOS=enhanceOS;
			this.tocrawlOS=tocrawlOS;
		}

		/*
		 * outputformat默认使用textoutputformat，textoutputformat默认使用lineRecordWriter，它会把kv写出，用\t分割kv，并换行
		 * 根据key的不同写到不同的文件中，每context.write()一组数据时就会调用一次write方法
		 * 通过流写到文件中去,可以是本地文件系统，也可以是hdfs文件
		 */
		@Override
		public void write(Text key, NullWritable value) throws IOException, InterruptedException {
			String result=key.toString();
			if(result.contains("tocrawl")){// 如果要写出的数据是待爬的url，则写入待爬清单文件 /logenhance/tocrawl/url.dat
				tocrawlOS.write(result.getBytes());
			}else{//如果要写出的数据是增强日志，则写入增强日志文件 /logenhance/enhancedlog/log.dat
				enhanceOS.write(result.getBytes());
			}
		}

		//关闭流
		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			if(enhanceOS!=null){
				enhanceOS.close();
			}
			if(tocrawlOS!=null){
				tocrawlOS.close();
			}
		}
		
	}
}
