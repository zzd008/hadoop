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
 * hdfs�ͻ���java API
 * windows���д���Ϊ����hdfs��ʱ�ᱨ��ָ���쳣��������Ϊ���л������� 
 * ����취��ϵͳ��������ָ��HADOOP_HOME��F:\BaiduNetdiskDownload\day06 hadoop\day06\���\hadoop-2.6.4  ������PATH�м���hadoop_home��bin
 * ����hadoop-2.6.4��linux�汾��������ģ����Ի����linux�ı��ؿ�native����windows������Ҫ����windows�ı��ؿ⣬����Ҫ����jar��windowsƽ̨hadoop-2.6.1.zip��ѹ���hadoop-2.6.1�е�bin��lib�滻��2.6.4��
 * ����ֱ��ָ��HADOOOP_HOMEΪhadoop-2.6.1
 * 
 * �ͻ���ȥ����hdfsʱ������һ���û���ݵ�
 * Ĭ������£�hdfs�ͻ���api���jvm�л�ȡһ����������Ϊ�Լ����û���ݣ�-DHADOOP_USER_NAME=hadoop
 * Ҳ�����ڹ���ͻ���fs����ʱ��ͨ���������ݽ�ȥ
 *
 */
public class HDFSClientDemo {
	
	Configuration conf=null;
	FileSystem fs = null;

	//�鿴configuration�д�����Щ����
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
		
		//new��ʱ������jar���µ�hdfs-default-xml�е�����  ���ȼ���conf.set >  �Զ��������ļ� hdfs-site.xml > jar���е�hdfs-default.xml
		conf = new Configuration();
		
		//Ҳ���Բ�����hdfs�ĵ�ַ������Ҫ��Hadoop�������ļ�core-site.xml��hdfs-site.xml���Ƶ���Ŀ���� ��Ȼ��Ĭ�϶�ȡjar���е�hdfs-default.xml��core-site.xml
//		conf.set("fs.defaultFS", "hdfs://master:9000");//�����ָ������ôFileSystemʹ�õ��Ǳ����ļ�ϵͳLocalFileSystem����
		
		conf.set("dfs.replication", "5");//���ø�����   conf.set >  �Զ��������ļ� hdfs-site.xml> jar���е�hdfs-default.xml
		
		//�õ�һ��hdfs�ļ�ϵͳ�Ŀͻ���ʵ������
		fs = FileSystem.get(conf);//ctrl+t�鿴�̳нṹ������hdfs����filesystem������DistributedFileSystem
//		fs=FileSystem.get(new URI("hdfs://master:9000"), conf);//ֱ��ָ��uri���������Բ���conf��ָ��hdfs�ĵ�ַ��
		fs=FileSystem.get(new URI("hdfs://master:9000"), conf,"hadoop");//ָ����hadoop��¼��hdfs
		
	}
	
	/*
	 * ����fs��ɾ�Ĳ�
	 */
	
	//�ļ��ϴ�copyFromLocalFile
	@Test
	public void testUpLoad() throws IllegalArgumentException, IOException{
		//Permission deny�Ľ������
		//��windows�����д��룬����hdfsʱjvm�����ϵͳ��������HADOOP_USER_NAME�����ϵͳ��������û�����õĻ��;ͻ���windows��Ĭ���û���zzdȥ��¼��hdfs������Ȩ�޲��� rw-r-r
		//1.����chmod 777 hdfs�ϵ��ļ�Ȩ�ޣ����ǲ���ȫ
		//2.����System.setProperty("HADOOP_USER_NAME", "hadoop")ָ������ʱ��ʲô��ݵ�¼��hdfs
	    //3.Ҳ�������õ��Ե�ϵͳ��������:����һ��HADOOP_USER_NAMEΪhadoop��Ҳ����ֱ�Ӹ�windowsϵͳ�û���Ϊhadoop
		//4.run configurations ָ��jvm���в�����-DHADOOP_USER_NAME=hadoop
		//5.�ڹ���ͻ���fs����ʱ��ͨ���������ݽ�ȥFileSystem.get(new URI("hdfs://master:9000"), conf,"hadoop");
		//�Ƽ���5��
		
		//�ļ����ھͻḲ��
		fs.copyFromLocalFile(new Path("C:/Users/zzd/Desktop/a.txt"), new Path("/a2.txt"));//·��Ҳ����д��hdfs��//master:9000/a2.txt
		fs.close();//close and flush	
	}
	
	//�ļ�����copyToLocalFile
		@Test
		public void testDownload() throws IllegalArgumentException, IOException{
			fs.copyToLocalFile(new Path("/a2.txt"), new Path("C:/Users/zzd/Desktop/a2.txt"));
//			fs.copyToLocalFile(false,new Path("/a2.txt"), new Path("C:/Users/zzd/Desktop/a2.txt"),true); ����Ҳ���ᱨ��ָ�룬true��ʾʹ��windows���ص�api
			fs.close();
		}	
	
		//�����ļ���mkdir
		@Test
		public void testMKdir() throws IllegalArgumentException, IOException{
			boolean b = fs.mkdirs(new Path("/testmkdir/aa"));//���testmkdir�����ڻ�һ�𴴽� �൱�������е� hdfs dfs -mkdr -p
			System.out.println(b);
			fs.close();
		}	
		
		//ɾ���ļ�rm
		@Test
		public void testDeldir() throws IllegalArgumentException, IOException{
			fs.delete(new Path("/testmkdir/aa"), true);//�ݹ�ɾ��
			fs.close();
		}	
		
		//�ݹ�鿴�ļ���Ϣ��Ԫ���ݣ�ls
		@Test
		public void testLS() throws IllegalArgumentException, IOException{
			/*
			 * ����ʹ�õ�����������list��ԭ��
			 * list��һ���ڴ����Ҫ�ȵ�list������ȫ��װ�꣨add����ŷ��أ����hdfs�������������ݣ���ôҪȫ��װ��list�в��ܷ��ظ��ͻ��ˣ�����̫���ˣ����������紫��ʱ
			 * ��iterator��zhiyou sangefa�㷢hasnext��next��remove�����������ǲ�������ݵģ���ͻ���ʲôʱ��ȥ����hasnext����ʲôʱ��ȥhdfs���жϣ�����next��ʱ����ȥhdfs��ȥȡ������һ��һ���Ļ�ȡ���ݶ�������list
			 * �������кܶ��������ʹ�õ����������Ǽ���
			 */
			//List<LocatedFileStatus>
			RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);//�ݹ��г��ļ�
			while(files.hasNext()){
				LocatedFileStatus file = files.next();
				file.getPath();
				file.getBlockLocations();
				file.getLen();
				file.getOwner();
				System.out.println(file);
				
				//�ļ��Ŀ���Ϣ:����getLength��ƫ����getOffset������Щ������getHosts
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
		
		//�ǵݹ�鿴�ļ���Ϣ��Ԫ���ݣ�
		@Test
		public void testLS2() throws IllegalArgumentException, IOException{
			FileStatus[] files = fs.listStatus(new Path("/"));//��Ϊ�Ƿǵݹ飬ֻ����ʾһ����Ŀ¼�ļ�������̤������ֱ�ӷ�������
			//���г��ļ����ļ���
			for(FileStatus f:files){
				f.getBlockSize();
				f.getPath().getName();
				System.out.println(f);
				f.isFile();//���ļ�
				f.isDirectory();//���ļ��У�Ŀ¼��
			}
			fs.close();
		}				
		
		
	//�ļ��Ƿ����
	@Test
	public void fileExists() throws IllegalArgumentException, IOException{
		String filename="hdfs://master:9000/a3.txt";
		Path p=new Path(filename);
		//�ж��ļ��Ǵ���
		if(fs.exists(p)){
			System.out.println("hdfs�ֲ�ʽ�ļ�ϵͳ�д��ڸ��ļ���");
		}else{
			System.out.println("hdfs�ֲ�ʽ�ļ�ϵͳ�в����ڸ��ļ���");
		}
	}	
	
		
		
	public static void main(String[] args) {
		
	}
}
