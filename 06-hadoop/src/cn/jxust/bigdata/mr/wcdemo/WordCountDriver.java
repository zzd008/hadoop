package cn.jxust.bigdata.mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;//注意别导错包了
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * 封装成job对象，然后程序被打成jar包、job.xml（配置信息）、job.split（切片文件规划） 提交给yarn去运行
 * 
 * 相当于一个yarn集群的客户端
 * 需要在此封装我们的mr程序的相关运行参数，指定jar包
 * 最后提交给yarn
 *
 */

/*
 * 因为linux的jdk为1.7所以程序也用1.7区编译
 * 把程序打成wc.jar 程序只有14kb，因为依赖的hadoop2.6.4jars包没放进去，因为程序只是引用了hadoop2.6.4jars(或者直接把需要的jar包拷贝到项目中，但是这样很浪费空间)
 * 这样在linux上java -cp wc.jar cn.jxust.bigdata.mr.wcdemo.WordCountDriver(主类) xxx(运行参数)就会报错，NoClassDefFoundError，因为找不到以来的hadoopjar包
 * 可以java -cp wc.jar:/home/hadoop/apps/hadoop2.6.4/share/hadoop/common/hadoop-comoon-2.1.jar:xxx.jar 把你要依赖的jar包全部加到classpath后面(用冒号接)，但是这样太麻烦
 * 所以直接用hadoop jar wc.jar cn.jxust.bigdata.mr.wcdemo.WordCountDriver xxx(运行参数) 去运行，因为linux已经配置好了hadoop的classpath(在/etc/profile)，这样他就能找到依赖的jar包
 * 或者直接打成runnable jar，这样就可以通过java  -jar wc.jar cn.jxust.bigdata.mr.wcdemo.WordCountDriver(主类) xxx(运行参数)去执行了，因为runnable jar会把相关jar包放到项目中，这时wc.jar就会很大
 * 但是java -jar xx还是不能运行，yarn会找不到程序的jar包  要把jar包路径写死ob.setJar("/home/hadoop/wc.jar")；并把hadoop的那些配置文件拷贝到项目下。
 */

/*
 * 程序输出效果
 * 运行后再linux上jps 会发现先出现mrappmaster 然后出现yarnchild(maptask、reduceask)
 * 运行完hdfs上只有一个输出文件，因为我们没有设置reducetask的数量，默认为1，所以只有一个文件
 * yarn网页：master：8088
 * 结果文件会按照key的字典顺序排序
 */
public class WordCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//环境配置
		Configuration conf = new Configuration();
		
		//本地模式运行mr程序，通过localjobrunner来运行mr程序,jvm通过线程来模拟map/reducetask
//		conf.set("mapreduce.framework.name", "local"); //可以不用配，默认是本地
//		conf.set("fs.defaultFS", "file:///");//访问本地文件 可以不用配，默认是本地
//		conf.set("fs.defaultFS", "hdfs://master:9000");//也可以访问hdfs的文件
		
		//集群运行模式，提交给yarn
		/*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resoucemanager.hostname", "master");
		conf.set("fs.defaultFS", "hdfs://master:9000");*/
		//直接把linux中的hadoop的配置文件拷贝到项目中，不要再自己通过conf配置参数，会连接不上yarn
		
		
		
		//理论上指定yarn的地址，在windows上就能运行，因为是客户端，但是windows平台很多东西不兼容没所以不行 还是要打成jar到linux上去运行
		//在linux上就不用设置conf了，因为通过hadoop jar去运行 Configuration能读到配置文件
		//如果在windows的eclipse上直接运行，不指定这句话，即不指定yarn，那么会在本地的模拟器上localjobrunner跑，而不是提交到集群的yarn上去
		/*conf.set("mapreduce.framework.name", "yarn"); 
		conf.set("yarn.resoucemanager.hostname", "master");*/
		
		Job job=Job.getInstance(conf);
//		Job job=new Job(conf,"MywordCount"); 方法过时
		
		/*
		 * 在linux上运行jar
		 * 如果是runnable jar：java -jar wc.jar(jar包) WordCountDriver(主类) xxx(运行参数)
		 * 普通jar包: java -cp(classpath) wc.jar(jar包) cn.jxust.bigdata.mr.wcdemo.WordCountDriver(主类) xxx(运行参数)
		 */
		//指定本程序的jar包所在的本地路径,只要运行main方法，就会把程序打成jar包提交给yarn
		//如果是通过java -jar 运行runnable jar，jar包路径就要写死
//		job.setJar("/home/hadoop/wc.jar");//这样把路径写死，那么程序一定要打包到这个路径下，而且名为wc.jar，这样通过运行main方法程序才能读到它，提交到yarn上
		
		//job.setJarByClass(WordCountDriver.class);//这样根据路径classbuilder获取jar包，jar包放到哪里都可以，通过 hadoop jar xxxx来运行main就能找到它
		//如果通过windows的eclispe运行，jar包一定要写死，因为eclispe不是通过jar去运行的
		job.setJar("C:/Users/zzd/Desktop/mr-jars/wc_windows.jar");//把程序先打成jar
		
		if (args == null || args.length == 0) {//运行时指定路径
			args = new String[2];
			args[0] = "hdfs://master:9000/wordcount/input/";
			args[1] = "hdfs://master:9000/wordcount/output";
		}
		
		/*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();//设置程序运行时的参数（输入输出文件路径）
		 //String[] otherArgs=new String[]{"/input","/output"}; //直接设置输入参数 输入输出文件路径）
		 if(otherArgs.length!=2){
			 System.err.println("运行参数错误，输入文件和输出文件！");//输出错误信息到控制台(红色字)
			 System.exit(2);// 退出程序 0--正常结束程序  非0--异常关闭程序；
		 }*/
		
		/*
		 * 配置job
		 */
		
		//指定本业务job要使用的mapper、Reducer业务类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//指定最终输出的数据的kv类型(有时不需要reduce，如将文件中的单词大写变小写)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		/*
		 * 小文件的优化策略：
		 * 假如使用切片的块大小为128mb，当有很多小文件时（不足128mb），默认会给每个小文件形成一个切片，这样就会有很多maptask，浪费，效率太低
		 * 指定使用哪种输入模板 CombineInputFormat可以将小文件逻辑上进行合并，将很多小文件形成一个切片，这样效率很高
		 * 不指定默认为textinputformat，它默认使用LineRecorderReader去读数据,都城kv格式给map()
		 */
		/*job.setInputFormatClass(CombineTextInputFormat.class);
		//设置最大/小切片大小  最好是128mb，会把很多小文件拼成128mb来算为一个切片
		CombineTextInputFormat.setMinInputSplitSize(job, 4*1024*1024);//4mb
		CombineTextInputFormat.setMaxInputSplitSize(job, 2*1024*1024);//2mb
*/		
		
		//指定使用哪个combiner
//		job.setCombinerClass(WordCountCombiner.class);//可以不写 因为逻辑和WordCountReducer一样
		job.setCombinerClass(WordCountReducer.class);
		
		/*
		 * 输入输出路径都在hdfs上
		 */
		//指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));//可以写死
//		FileInputFormat.setInputPaths(job, new Path(args[0]),new Path(args[1]));//可以指定多个输入路径，用逗号隔开
		
		//指定job的输出结果所在目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
//		job.submit();//这样写不好，程序提交给yarn集群中去执行，客户端无法判断程序是否运行成功
		//这个方法会阻塞，直到程序在yarn中运行完，返回结果
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);//0正常退出 非0不正常退出
		//$?可以再shell脚本中获取程序的退出码
	}
}
