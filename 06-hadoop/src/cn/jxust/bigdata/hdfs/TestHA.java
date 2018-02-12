package cn.jxust.bigdata.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * ����HA�ϵ�hdfs 
 * �߿��� ����namenode�������ռ���bi����������������nn
 */
public class TestHA {
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);
		conf.set("fs.defaultFS", "hdfs://bi");//���������bi����hdfs��·���ᱨ������Ҫ��HA�������ļ��ŵ�src�£���������֪��bi��һ�������ռ�
		fs.copyFromLocalFile(new Path("c:/a.txt"), new Path("/hello/s"));
		fs.close();
	}
}
