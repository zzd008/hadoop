package cn.jxust.bigdata.mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;//ע��𵼴����
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * ��װ��job����Ȼ����򱻴��jar����job.xml��������Ϣ����job.split����Ƭ�ļ��滮�� �ύ��yarnȥ����
 * 
 * �൱��һ��yarn��Ⱥ�Ŀͻ���
 * ��Ҫ�ڴ˷�װ���ǵ�mr�����������в�����ָ��jar��
 * ����ύ��yarn
 *
 */

/*
 * ��Ϊlinux��jdkΪ1.7���Գ���Ҳ��1.7������
 * �ѳ�����wc.jar ����ֻ��14kb����Ϊ������hadoop2.6.4jars��û�Ž�ȥ����Ϊ����ֻ��������hadoop2.6.4jars(����ֱ�Ӱ���Ҫ��jar����������Ŀ�У������������˷ѿռ�)
 * ������linux��java -cp wc.jar cn.jxust.bigdata.mr.wcdemo.WordCountDriver(����) xxx(���в���)�ͻᱨ��NoClassDefFoundError����Ϊ�Ҳ���������hadoopjar��
 * ����java -cp wc.jar:/home/hadoop/apps/hadoop2.6.4/share/hadoop/common/hadoop-comoon-2.1.jar:xxx.jar ����Ҫ������jar��ȫ���ӵ�classpath����(��ð�Ž�)����������̫�鷳
 * ����ֱ����hadoop jar wc.jar cn.jxust.bigdata.mr.wcdemo.WordCountDriver xxx(���в���) ȥ���У���Ϊlinux�Ѿ����ú���hadoop��classpath(��/etc/profile)�������������ҵ�������jar��
 * ����ֱ�Ӵ��runnable jar�������Ϳ���ͨ��java  -jar wc.jar cn.jxust.bigdata.mr.wcdemo.WordCountDriver(����) xxx(���в���)ȥִ���ˣ���Ϊrunnable jar������jar���ŵ���Ŀ�У���ʱwc.jar�ͻ�ܴ�
 * ����java -jar xx���ǲ������У�yarn���Ҳ��������jar��  Ҫ��jar��·��д��ob.setJar("/home/hadoop/wc.jar")������hadoop����Щ�����ļ���������Ŀ�¡�
 */

/*
 * �������Ч��
 * ���к���linux��jps �ᷢ���ȳ���mrappmaster Ȼ�����yarnchild(maptask��reduceask)
 * ������hdfs��ֻ��һ������ļ�����Ϊ����û������reducetask��������Ĭ��Ϊ1������ֻ��һ���ļ�
 * yarn��ҳ��master��8088
 * ����ļ��ᰴ��key���ֵ�˳������
 */
public class WordCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//��������
		Configuration conf = new Configuration();
		
		//����ģʽ����mr����ͨ��localjobrunner������mr����,jvmͨ���߳���ģ��map/reducetask
//		conf.set("mapreduce.framework.name", "local"); //���Բ����䣬Ĭ���Ǳ���
//		conf.set("fs.defaultFS", "file:///");//���ʱ����ļ� ���Բ����䣬Ĭ���Ǳ���
//		conf.set("fs.defaultFS", "hdfs://master:9000");//Ҳ���Է���hdfs���ļ�
		
		//��Ⱥ����ģʽ���ύ��yarn
		/*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resoucemanager.hostname", "master");
		conf.set("fs.defaultFS", "hdfs://master:9000");*/
		//ֱ�Ӱ�linux�е�hadoop�������ļ���������Ŀ�У���Ҫ���Լ�ͨ��conf���ò����������Ӳ���yarn
		
		
		
		//������ָ��yarn�ĵ�ַ����windows�Ͼ������У���Ϊ�ǿͻ��ˣ�����windowsƽ̨�ܶණ��������û���Բ��� ����Ҫ���jar��linux��ȥ����
		//��linux�ϾͲ�������conf�ˣ���Ϊͨ��hadoop jarȥ���� Configuration�ܶ��������ļ�
		//�����windows��eclipse��ֱ�����У���ָ����仰������ָ��yarn����ô���ڱ��ص�ģ������localjobrunner�ܣ��������ύ����Ⱥ��yarn��ȥ
		/*conf.set("mapreduce.framework.name", "yarn"); 
		conf.set("yarn.resoucemanager.hostname", "master");*/
		
		Job job=Job.getInstance(conf);
//		Job job=new Job(conf,"MywordCount"); ������ʱ
		
		/*
		 * ��linux������jar
		 * �����runnable jar��java -jar wc.jar(jar��) WordCountDriver(����) xxx(���в���)
		 * ��ͨjar��: java -cp(classpath) wc.jar(jar��) cn.jxust.bigdata.mr.wcdemo.WordCountDriver(����) xxx(���в���)
		 */
		//ָ���������jar�����ڵı���·��,ֻҪ����main�������ͻ�ѳ�����jar���ύ��yarn
		//�����ͨ��java -jar ����runnable jar��jar��·����Ҫд��
//		job.setJar("/home/hadoop/wc.jar");//������·��д������ô����һ��Ҫ��������·���£�������Ϊwc.jar������ͨ������main����������ܶ��������ύ��yarn��
		
		//job.setJarByClass(WordCountDriver.class);//��������·��classbuilder��ȡjar����jar���ŵ����ﶼ���ԣ�ͨ�� hadoop jar xxxx������main�����ҵ���
		//���ͨ��windows��eclispe���У�jar��һ��Ҫд������Ϊeclispe����ͨ��jarȥ���е�
		job.setJar("C:/Users/zzd/Desktop/mr-jars/wc_windows.jar");//�ѳ����ȴ��jar
		
		if (args == null || args.length == 0) {//����ʱָ��·��
			args = new String[2];
			args[0] = "hdfs://master:9000/wordcount/input/";
			args[1] = "hdfs://master:9000/wordcount/output";
		}
		
		/*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();//���ó�������ʱ�Ĳ�������������ļ�·����
		 //String[] otherArgs=new String[]{"/input","/output"}; //ֱ������������� ��������ļ�·����
		 if(otherArgs.length!=2){
			 System.err.println("���в������������ļ�������ļ���");//���������Ϣ������̨(��ɫ��)
			 System.exit(2);// �˳����� 0--������������  ��0--�쳣�رճ���
		 }*/
		
		/*
		 * ����job
		 */
		
		//ָ����ҵ��jobҪʹ�õ�mapper��Reducerҵ����
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//ָ��������������ݵ�kv����(��ʱ����Ҫreduce���罫�ļ��еĵ��ʴ�д��Сд)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		/*
		 * С�ļ����Ż����ԣ�
		 * ����ʹ����Ƭ�Ŀ��СΪ128mb�����кܶ�С�ļ�ʱ������128mb����Ĭ�ϻ��ÿ��С�ļ��γ�һ����Ƭ�������ͻ��кܶ�maptask���˷ѣ�Ч��̫��
		 * ָ��ʹ����������ģ�� CombineInputFormat���Խ�С�ļ��߼��Ͻ��кϲ������ܶ�С�ļ��γ�һ����Ƭ������Ч�ʺܸ�
		 * ��ָ��Ĭ��Ϊtextinputformat����Ĭ��ʹ��LineRecorderReaderȥ������,����kv��ʽ��map()
		 */
		/*job.setInputFormatClass(CombineTextInputFormat.class);
		//�������/С��Ƭ��С  �����128mb����Ѻܶ�С�ļ�ƴ��128mb����Ϊһ����Ƭ
		CombineTextInputFormat.setMinInputSplitSize(job, 4*1024*1024);//4mb
		CombineTextInputFormat.setMaxInputSplitSize(job, 2*1024*1024);//2mb
*/		
		
		//ָ��ʹ���ĸ�combiner
//		job.setCombinerClass(WordCountCombiner.class);//���Բ�д ��Ϊ�߼���WordCountReducerһ��
		job.setCombinerClass(WordCountReducer.class);
		
		/*
		 * �������·������hdfs��
		 */
		//ָ��job������ԭʼ�ļ�����Ŀ¼
		FileInputFormat.setInputPaths(job, new Path(args[0]));//����д��
//		FileInputFormat.setInputPaths(job, new Path(args[0]),new Path(args[1]));//����ָ���������·�����ö��Ÿ���
		
		//ָ��job������������Ŀ¼
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//��job�����õ���ز������Լ�job���õ�java�����ڵ�jar�����ύ��yarnȥ����
//		job.submit();//����д���ã������ύ��yarn��Ⱥ��ȥִ�У��ͻ����޷��жϳ����Ƿ����гɹ�
		//���������������ֱ��������yarn�������꣬���ؽ��
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);//0�����˳� ��0�������˳�
		//$?������shell�ű��л�ȡ������˳���
	}
}
