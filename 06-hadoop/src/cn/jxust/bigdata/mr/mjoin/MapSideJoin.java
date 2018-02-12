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
 * mapd端join 解决数据倾斜
 * hadoop提供了一个分布式缓存，distributedCache，把商品文件放到分布式缓存中，哪台机器去运行maptask，它就把商品字典发到那台机器的task的工作目录下（container）。
 */
public class MapSideJoin {
	
	static class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		//存放商品信息
		Map<String,String> pdInfoMap=new HashMap<String,String>();
		Text k=new Text();
		
		//阅读mapper源码发现，mapper通过线程去调用map方法  run方法中先加载setup()做初始化工作，然后调用map()处理逻辑，map()全部结束后调用cleanup()方法做清理
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			//通过这几句代码可以获取到cache file的本地绝对路径，测试验证用
//			Path[] files = context.getLocalCacheFiles();//过期
			URI[] cacheFiles = context.getCacheFiles();
			String localpath = cacheFiles[0].toString();
			System.out.println("缓存文件被放在了task的工作目录下，它的源目录是："+localpath);
			
			//加载产品表 因为已经缓存到task的工作目录了，这段代码就是在task的工作目录运行的，所以直接写文件名就可以了
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("product.data")));
			String line;
			while(StringUtils.isNotEmpty(line=br.readLine())){//非空 
				String[] fields = line.split("\t");
				pdInfoMap.put(fields[0],fields[1]+"\t"+fields[2]+"\t"+fields[3]);//如果字段很多，可以封装成一个bean
			}
			br.close();
		}
		
		//setup()已经初始化了商品hashmap，这时就可以在map实现join逻辑了
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String OrderLine = value.toString();
			String[] fields = OrderLine.split("\t");
			String pid=fields[2];
			//根据商品pid从商品pdInfoMap中查询商品信息
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

		//不用设置reducer,但是不写默认还是有一个reduce的，所以设置reducetask数量为0
		job.setNumReduceTasks(0);
		
		//因为map输出就是最终结果，所以直接设置outputkey、outputvalue
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		/*job.setMapOutputKeyClass(Text.class); //有时不需要reduce，map可以直接当输出
		job.setMapOutputValueClass(NullWritable.class);*/
		
		FileInputFormat.setInputPaths(job, new Path("F:/mrdata/mapjoin/input"));
		FileOutputFormat.setOutputPath(job, new Path("F:/mrdata/mapjoin/output"));
	
		//指定你需要缓存到所有的maptask运行节点的工作目录中去的缓存文件
		/*job.addArchiveToClassPath(archive);*/  //缓存jar包（压缩包）到tsk运行节点的classpath中去，这样task工作时就能通过加载classpath去下载文件
		/*job.addFileToClassPath(file);*/   	//缓存普通文件到task运行节点的classpath中
		/*job.addCacheArchive(uri);*/       //缓存压缩包到task运行节点的工作目录
		/*job.addCacheFile(uri)*/         //缓存普通文件到task运行节点的工作目录
		
		//将商品表文件缓存到task工作节点的工作目录中去 windows不支持file://F:/mrdata和file:///F:/mrdata/
		job.addCacheFile(new URI("file:/F:/mrdata/mapjoin/cachefile/product.data"));//路径是uri格式 可以是本地文件也可以是hdfs文件			   //缓存普通文件到task运行节点的工作目录
		
		System.exit(job.waitForCompletion(true)?0:-1);
	}
}
