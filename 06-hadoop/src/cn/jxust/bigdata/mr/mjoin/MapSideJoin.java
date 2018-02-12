package cn.jxust.bigdata.mr.mjoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * mapd��join ���������б
 * hadoop�ṩ��һ���ֲ�ʽ���棬distributedCache������Ʒ�ļ��ŵ��ֲ�ʽ�����У���̨����ȥ����maptask�����Ͱ���Ʒ�ֵ䷢����̨������task�Ĺ���Ŀ¼�£�container����
 */
public class MapSideJoin {
	
	static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		//�����Ʒ��Ϣ
		Map<String,String> pdInfoMap=new HashMap<String,String>();
		Text k=new Text();
		
		//�Ķ�mapperԴ�뷢�֣�mapperͨ���߳�ȥ����map����  run�������ȼ���setup()����ʼ��������Ȼ�����map()�����߼���map()ȫ�����������cleanup()����������
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			//ͨ���⼸�������Ի�ȡ��cache file�ı��ؾ���·����������֤��
//			Path[] files = context.getLocalCacheFiles();//����
			URI[] cacheFiles = context.getCacheFiles();
			String localpath = cacheFiles[0].toString();
			System.out.println("�����ļ���������task�Ĺ���Ŀ¼�£�����ԴĿ¼�ǣ�"+localpath);
			
			//���ز�Ʒ�� ��Ϊ�Ѿ����浽task�Ĺ���Ŀ¼�ˣ���δ��������task�Ĺ���Ŀ¼���еģ�����ֱ��д�ļ����Ϳ�����
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("product.data")));
			String line;
			while(StringUtils.isNotEmpty(line=br.readLine())){//�ǿ� 
				String[] fields = line.split("\t");
				pdInfoMap.put(fields[0],fields[1]+"\t"+fields[2]+"\t"+fields[3]);//����ֶκܶ࣬���Է�װ��һ��bean
			}
			br.close();
		}
		
		//setup()�Ѿ���ʼ������Ʒhashmap����ʱ�Ϳ�����mapʵ��join�߼���
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String OrderLine = value.toString();
			String[] fields = OrderLine.split("\t");
			String pid=fields[2];
			//������Ʒpid����ƷpdInfoMap�в�ѯ��Ʒ��Ϣ
			String pdInfo = pdInfoMap.get(pid);
			
			k.set(OrderLine+"\t"+pdInfo);
			
			context.write(k, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		
		Job job=Job.getInstance();
		
		job.setJarByClass(MapSideJoin.class);
		
		job.setMapperClass(MapSideJoinMapper.class);

		//��������reducer,���ǲ�дĬ�ϻ�����һ��reduce�ģ���������reducetask����Ϊ0
		job.setNumReduceTasks(0);
		
		//��Ϊmap����������ս��������ֱ������outputkey��outputvalue
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		/*job.setMapOutputKeyClass(Text.class); //��ʱ����Ҫreduce��map����ֱ�ӵ����
		job.setMapOutputValueClass(NullWritable.class);*/
		
		FileInputFormat.setInputPaths(job, new Path("F:/mrdata/mapjoin/input"));
		FileOutputFormat.setOutputPath(job, new Path("F:/mrdata/mapjoin/output"));
	
		//ָ������Ҫ���浽���е�maptask���нڵ�Ĺ���Ŀ¼��ȥ�Ļ����ļ�
		/*job.addArchiveToClassPath(archive);*/  //����jar����ѹ��������tsk���нڵ��classpath��ȥ������task����ʱ����ͨ������classpathȥ�����ļ�
		/*job.addFileToClassPath(file);*/   	//������ͨ�ļ���task���нڵ��classpath��
		/*job.addCacheArchive(uri);*/       //����ѹ������task���нڵ�Ĺ���Ŀ¼
		/*job.addCacheFile(uri)*/         //������ͨ�ļ���task���нڵ�Ĺ���Ŀ¼
		
		//����Ʒ���ļ����浽task�����ڵ�Ĺ���Ŀ¼��ȥ windows��֧��file://F:/mrdata��file:///F:/mrdata/
		job.addCacheFile(new URI("file:/F:/mrdata/mapjoin/cachefile/product.data"));//·����uri��ʽ �����Ǳ����ļ�Ҳ������hdfs�ļ�			   //������ͨ�ļ���task���нڵ�Ĺ���Ŀ¼
		
		System.exit(job.waitForCompletion(true)?0:-1);
	}
}
