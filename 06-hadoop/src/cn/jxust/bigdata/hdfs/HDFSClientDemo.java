package cn.jxust.bigdata.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

/*
 * hdfs客户端java API
 * windows运行代码为访问hdfs有时会报空指针异常，这是因为运行环境问题 
 * 解决办法：系统环境变量指定HADOOP_HOME：F:\BaiduNetdiskDownload\day06 hadoop\day06\软件\hadoop-2.6.4  还有在PATH中加入hadoop_home的bin
 * 但是hadoop-2.6.4是linux版本编译过来的，所以会调用linux的本地库native而在windows上运行要调用windows的本地库，所以要把无jar版windows平台hadoop-2.6.1.zip解压后的hadoop-2.6.1中的bin和lib替换到2.6.4中
 * 或者直接指定HADOOOP_HOME为hadoop-2.6.1
 * 
 * 客户端去操作hdfs时，是有一个用户身份的
 * 默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份：-DHADOOP_USER_NAME=hadoop
 * 也可以在构造客户端fs对象时，通过参数传递进去
 *
 */
public class HDFSClientDemo {
	
	Configuration conf=null;
	FileSystem fs = null;

	//查看configuration中存了哪些配置
	@Test
	public void testConf(){
		 Iterator<Entry<String, String>> iterator = conf.iterator();
		 while(iterator.hasNext()){
			 Entry<String, String> entry = iterator.next();
			 System.out.println(entry.getKey()+" : "+entry.getValue());
		 }
	}
	
	@Before
	public void init() throws IOException, URISyntaxException, InterruptedException{
		
		//new的时候会加载jar包下的hdfs-default-xml中的配置  优先级：conf.set >  自定义配置文件 hdfs-site.xml > jar包中的hdfs-default.xml
		conf = new Configuration();
		
		//也可以不设置hdfs的地址，但是要把Hadoop的配置文件core-site.xml和hdfs-site.xml复制到项目的下 不然会默认读取jar包中的hdfs-default.xml和core-site.xml
//		conf.set("fs.defaultFS", "hdfs://master:9000");//如果不指定，那么FileSystem使用的是本地文件系统LocalFileSystem对象
		
		conf.set("dfs.replication", "5");//设置副本数   conf.set >  自定义配置文件 hdfs-site.xml> jar包中的hdfs-default.xml
		
		//拿到一个hdfs文件系统的客户端实例对象
		fs = FileSystem.get(conf);//ctrl+t查看继承结构，操作hdfs的是filesystem的子类DistributedFileSystem
//		fs=FileSystem.get(new URI("hdfs://master:9000"), conf);//直接指定uri，这样可以不在conf中指定hdfs的地址了
		fs=FileSystem.get(new URI("hdfs://master:9000"), conf,"hadoop");//指定以hadoop登录到hdfs
		
	}
	
	/*
	 * 利用fs增删改查
	 */
	
	//文件上传copyFromLocalFile
	@Test
	public void testUpLoad() throws IllegalArgumentException, IOException{
		//Permission deny的解决方法
		//在windows上运行代码，访问hdfs时jvm会查找系统环境变量HADOOP_USER_NAME，如果系统环境变量没有配置的话就就会以windows的默认用户名zzd去登录到hdfs，但是权限不够 rw-r-r
		//1.可以chmod 777 hdfs上的文件权限，但是不安全
		//2.可以System.setProperty("HADOOP_USER_NAME", "hadoop")指定运行时以什么身份登录到hdfs
	    //3.也可以配置电脑的系统环境变量:增加一个HADOOP_USER_NAME为hadoop，也可以直接改windows系统用户名为hadoop
		//4.run configurations 指定jvm运行参数：-DHADOOP_USER_NAME=hadoop
		//5.在构造客户端fs对象时，通过参数传递进去FileSystem.get(new URI("hdfs://master:9000"), conf,"hadoop");
		//推荐第5种
		
		//文件存在就会覆盖
		fs.copyFromLocalFile(new Path("C:/Users/zzd/Desktop/a.txt"), new Path("/a2.txt"));//路径也可以写成hdfs：//master:9000/a2.txt
		fs.close();//close and flush	
	}
	
	//文件下载copyToLocalFile
		@Test
		public void testDownload() throws IllegalArgumentException, IOException{
			fs.copyToLocalFile(new Path("/a2.txt"), new Path("C:/Users/zzd/Desktop/a2.txt"));
//			fs.copyToLocalFile(false,new Path("/a2.txt"), new Path("C:/Users/zzd/Desktop/a2.txt"),true); 这样也不会报空指针，true表示使用windows本地的api
			fs.close();
		}	
	
		//创建文件夹mkdir
		@Test
		public void testMKdir() throws IllegalArgumentException, IOException{
			boolean b = fs.mkdirs(new Path("/testmkdir/aa"));//如果testmkdir不存在会一起创建 相当于命令中的 hdfs dfs -mkdr -p
			System.out.println(b);
			fs.close();
		}	
		
		//删除文件rm
		@Test
		public void testDeldir() throws IllegalArgumentException, IOException{
			fs.delete(new Path("/testmkdir/aa"), true);//递归删除
			fs.close();
		}	
		
		//递归查看文件信息（元数据）ls
		@Test
		public void testLS() throws IllegalArgumentException, IOException{
			/*
			 * 这里使用迭代器而不是list的原因：
			 * list是一个内存对象，要等到list中数据全部装完（add）后才返回，如果hdfs中有上亿条数据，那么要全部装到list中才能返回给客户端，这样太慢了，尤其是网络传输时
			 * 而iterator中zhiyou sangefa你发hasnext、next、remove，迭代器中是不会存数据的，你客户端什么时候去调用hasnext，就什么时候去hdfs中判断，调用next的时候再去hdfs中去取，这样一条一条的获取数据而不是像list
			 * 大数据中很多情况都是使用迭代器而不是集合
			 */
			//List<LocatedFileStatus>
			RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);//递归列出文件
			while(files.hasNext()){
				LocatedFileStatus file = files.next();
				file.getPath();
				file.getBlockLocations();
				file.getLen();
				file.getOwner();
				System.out.println(file);
				
				//文件的块信息:长度getLength，偏移量getOffset，在哪些机器上getHosts
				BlockLocation[] blockLocations = file.getBlockLocations();
				for (BlockLocation bl : blockLocations) {
					System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
					String[] hosts = bl.getHosts();
					for (String host : hosts) {
						System.out.println(host);
					}
				}

			}
			fs.close();
		}	
		
		//非递归查看文件信息（元数据）
		@Test
		public void testLS2() throws IllegalArgumentException, IOException{
			FileStatus[] files = fs.listStatus(new Path("/"));//因为是非递归，只能显示一级的目录文件，不会踏跺所以直接返回数组
			//会列出文件和文件夹
			for(FileStatus f:files){
				f.getBlockSize();
				f.getPath().getName();
				System.out.println(f);
				f.isFile();//是文件
				f.isDirectory();//是文件夹（目录）
			}
			fs.close();
		}				
		
		
	//文件是否存在
	@Test
	public void fileExists() throws IllegalArgumentException, IOException{
		String filename="hdfs://master:9000/a3.txt";
		Path p=new Path(filename);
		//判断文件是存在
		if(fs.exists(p)){
			System.out.println("hdfs分布式文件系统中存在该文件！");
		}else{
			System.out.println("hdfs分布式文件系统中不存在该文件！");
		}
	}	
	
		
		
	public static void main(String[] args) {
		
	}
}
