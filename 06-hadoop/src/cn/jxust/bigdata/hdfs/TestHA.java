package cn.jxust.bigdata.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * 访问HA上的hdfs 
 * 高可用 两个namenode，命名空间是bi，下面有两个两个nn
 */
public class TestHA {
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);
		conf.set("fs.defaultFS", "hdfs://bi");//这样它会把bi当成hdfs的路径会报错，所以要把HA的配置文件放到src下，这样他才知道bi是一个命名空间
		fs.copyFromLocalFile(new Path("c:/a.txt"), new Path("/hello/s"));
		fs.close();
	}
}
