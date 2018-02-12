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
 * reducetask���������ʱ�Ż����outputformat�����ǳ����в�ָ��reducetask����ômaptask�ͻ�ֱ���������outputformat���������shuffle�׶Σ����ݰ���ʲô˳�����룬�Ͱ���ʲô˳�����
 * 
 * �Զ������ģ�� ��Ϊ��������ļ�ϵͳ�����Լ̳�FileOutputFormat ���п�����������ݿ�Ⱥܶ�ط�
 * maptask����reducetask���������ʱ���ȵ���OutputFormat��getRecordWriter�����õ�һ��RecordWriter
 * Ȼ��ÿcontext.write()һ������ʱ�ͻ����һ��RecordWriter��write(k,v)����������д��
 * ���Ի�Ҫ�Զ���һ��RecordWriter���̳�RecordWriter����дwrite����
 */
public class LogEnhanceOutPutFormat extends FileOutputFormat<Text, NullWritable>{
	
	//��ȡRecordWriter
	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		FileSystem fs=FileSystem.get(context.getConfiguration());//ͨ��context����ȡ�����Configuration����
		
		//�������·��ָ��Ϊ����  
		Path enhancePath=new Path("F:/mrdata/weblogenhance/enhance/log.dat");//��ǿ��־·��
		Path tocrawlPath=new Path("F:/mrdata/weblogenhance/tocrawl/url.dat");//��������·��
		
		FSDataOutputStream enhanceOS = fs.create(enhancePath);
		FSDataOutputStream tocrawlOS = fs.create(tocrawlPath);
		
		return new MyRecordWriter(enhanceOS,tocrawlOS);//ͨ�����캯������write������
	}
	
	static class MyRecordWriter extends RecordWriter<Text, NullWritable>{
		FSDataOutputStream enhanceOS = null;
		FSDataOutputStream tocrawlOS =null;
				
		public MyRecordWriter(FSDataOutputStream enhanceOS, FSDataOutputStream tocrawlOS) {
			this.enhanceOS=enhanceOS;
			this.tocrawlOS=tocrawlOS;
		}

		/*
		 * outputformatĬ��ʹ��textoutputformat��textoutputformatĬ��ʹ��lineRecordWriter�������kvд������\t�ָ�kv��������
		 * ����key�Ĳ�ͬд����ͬ���ļ��У�ÿcontext.write()һ������ʱ�ͻ����һ��write����
		 * ͨ����д���ļ���ȥ,�����Ǳ����ļ�ϵͳ��Ҳ������hdfs�ļ�
		 */
		@Override
		public void write(Text key, NullWritable value) throws IOException, InterruptedException {
			String result=key.toString();
			if(result.contains("tocrawl")){// ���Ҫд���������Ǵ�����url����д������嵥�ļ� /logenhance/tocrawl/url.dat
				tocrawlOS.write(result.getBytes());
			}else{//���Ҫд������������ǿ��־����д����ǿ��־�ļ� /logenhance/enhancedlog/log.dat
				enhanceOS.write(result.getBytes());
			}
		}

		//�ر���
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
