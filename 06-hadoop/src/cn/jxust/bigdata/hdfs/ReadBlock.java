package cn.jxust.bigdata.hdfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * 场景编程
 * 在mapreduce 、spark等运算框架中，有一个核心思想就是将运算移往数据，或者说，就是要在并发计算中尽可能让运算本地化，这就需要获取数据所在位置的信息并进行相应范围读取
 * 以下模拟实现：获取一个文件的所有block位置信息，然后读取指定block中的内容
 */
public class ReadBlock {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		Configuration conf=new Configuration();
		FileSystem f=FileSystem.get(new URI("hdfs://master:9000"), conf, "hadoop");
		
		//上传test.txt到hdfs 文件很小没有被切块
		f.copyFromLocalFile(new Path("C:/Users/zzd/Desktop/test.txt"), new Path("/"));
		
		FileStatus[] fileStatus = f.listStatus(new Path("/test.txt"));
		FileStatus fs=fileStatus[0];
		//获取块信息
		BlockLocation[] fileBlockLocations = f.getFileBlockLocations(fs, 0, fs.getLen());//也可以f.listfile 然后getBlockLocations liststatus方法中没有这个方法
		System.out.println(fileBlockLocations.length);//因为文件很小没有被分块，所以为1
		
		BlockLocation bl = fileBlockLocations[0];
		//因为没有被分块，所以起始偏移量和长度为 0 26
		System.out.println(bl.getOffset()+" "+bl.getLength()+" ");
		
		//读取5-20个数据长度
		FSDataInputStream in = f.open(new Path("/test.txt"));
		FileOutputStream out = new FileOutputStream("C:/Users/zzd/Desktop/test1.txt");
		
		//方法一
//		IOUtils.copyLarge(in, out, 5, 20-5);//这是最好的方法 ：从第5个开始读，读20-5个
		
		//方法二
		/*in.seek(5);//从第五个字节开始读
		int len;
		int count=0;
		while((len=in.read())!=-1){ //in.read()读一次，加入读到a 他返回arsicc码 97
			System.out.println(len);
			out.write(len);//写出
			count+=1;
//			if(count>=5)
			if(count>15) return;
		}*/
		
		/*while((len=in.read(b))!=-1){ 这样不行，会一下子全部读到数组b中，长度超过了20,可以把数组长度从1024减小为5或者一次只读一个字节
			out.write(b, 0, len);
			count+=len;
			if(count>20) return;
		}*/
		
		/*out.flush();
		out.close();
		in.close();*/
	}
}
