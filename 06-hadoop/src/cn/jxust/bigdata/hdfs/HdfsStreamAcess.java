package cn.jxust.bigdata.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/*
 * 用流的方式操作hdfs
 * 可以实现读取指定偏移量范围的数据
 */
public class HdfsStreamAcess {

	Configuration conf=null;
	FileSystem f=null;
	
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException{
		conf=new Configuration();
		f=FileSystem.get(new URI("hdfs://master:9000"), conf, "hadoop");
	}
	
	/*
	 * 上传文件
	 */
	@Test
	public void testUplocad() throws IllegalArgumentException, IOException{
		//FSDataOutputStream输出流，用于操纵hdfs
		FSDataOutputStream outputstream = f.create(new Path("/aa.txt"), true);//覆盖写
		//获取本地输入流
		FileInputStream fileInputStream = new FileInputStream("C:/Users/zzd/Desktop/a2.txt");
		//可以用while循环写，还可以包装一下流，但是用ioutils工具更方便
		/*byte[] b=new byte[1024];
		int len;
		while((len=fileInputStream.read(b))!=-1){
			outputstream.write(b, 0, len);
		}*/
		
		//可以用org.apache.hadoop.io.IOUtils 
//		IOUtils.copyBytes(fileInputStream, outputstream, conf); 
//		IOUtils.copyBytes(fileInputStream, outputstream, 2048); //缓冲数组大小为2048字节byte
		//但是org.apache.commons.io.IOUtils的更方便
		IOUtils.copy(fileInputStream, outputstream);//会自动帮你关流
	}
	
	/*
	 * 下载文件
	 */
	@Test
	public void testDownload() throws IllegalArgumentException, IOException{
		//FSDataInputStream输入流，用于操纵hdfs
		FSDataInputStream in = f.open(new Path("/aa.txt"));
		//获取本地输出流
		FileOutputStream out = new FileOutputStream("C:/Users/zzd/Desktop/aa.txt");
		IOUtils.copy(in, out);
		
	}
	
	/*
	 * 随机读写
	 */
	@Test
	public void testRandomAcess() throws IllegalArgumentException, IOException{
		//FSDataInputStream输入流，用于操纵hdfs
		FSDataInputStream in = f.open(new Path("/aa.txt"));
		in.seek(4);//从第4个字节开始读直到文件末尾
		//获取本地输出流
		FileOutputStream out = new FileOutputStream("C:/Users/zzd/Desktop/b.txt");
		IOUtils.copy(in, out);
//		IOUtils.copyLarge(in, out, 4, 10); 或者用这个方法 从第四个开始读，读10个
		//如果要读60mb-120mb，则开始时指定seek(60*1024*1024) 然后while循环中用一个计数器，让它读到120mb
		
	}
	
	/*
	 * 显示hdfs上文件的内容
	 */
	@Test
	public void testCat() throws IllegalArgumentException, IOException{
		FSDataInputStream in = f.open(new Path("/aa.txt"));
		org.apache.hadoop.io.IOUtils.copyBytes(in, System.out, 1024);//system.out是一个printstream输出流
	}

}
