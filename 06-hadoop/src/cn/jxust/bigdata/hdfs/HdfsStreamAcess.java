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
 * �����ķ�ʽ����hdfs
 * ����ʵ�ֶ�ȡָ��ƫ������Χ������
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
	 * �ϴ��ļ�
	 */
	@Test
	public void testUplocad() throws IllegalArgumentException, IOException{
		//FSDataOutputStream����������ڲ���hdfs
		FSDataOutputStream outputstream = f.create(new Path("/aa.txt"), true);//����д
		//��ȡ����������
		FileInputStream fileInputStream = new FileInputStream("C:/Users/zzd/Desktop/a2.txt");
		//������whileѭ��д�������԰�װһ������������ioutils���߸�����
		/*byte[] b=new byte[1024];
		int len;
		while((len=fileInputStream.read(b))!=-1){
			outputstream.write(b, 0, len);
		}*/
		
		//������org.apache.hadoop.io.IOUtils 
//		IOUtils.copyBytes(fileInputStream, outputstream, conf); 
//		IOUtils.copyBytes(fileInputStream, outputstream, 2048); //���������СΪ2048�ֽ�byte
		//����org.apache.commons.io.IOUtils�ĸ�����
		IOUtils.copy(fileInputStream, outputstream);//���Զ��������
	}
	
	/*
	 * �����ļ�
	 */
	@Test
	public void testDownload() throws IllegalArgumentException, IOException{
		//FSDataInputStream�����������ڲ���hdfs
		FSDataInputStream in = f.open(new Path("/aa.txt"));
		//��ȡ���������
		FileOutputStream out = new FileOutputStream("C:/Users/zzd/Desktop/aa.txt");
		IOUtils.copy(in, out);
		
	}
	
	/*
	 * �����д
	 */
	@Test
	public void testRandomAcess() throws IllegalArgumentException, IOException{
		//FSDataInputStream�����������ڲ���hdfs
		FSDataInputStream in = f.open(new Path("/aa.txt"));
		in.seek(4);//�ӵ�4���ֽڿ�ʼ��ֱ���ļ�ĩβ
		//��ȡ���������
		FileOutputStream out = new FileOutputStream("C:/Users/zzd/Desktop/b.txt");
		IOUtils.copy(in, out);
//		IOUtils.copyLarge(in, out, 4, 10); ������������� �ӵ��ĸ���ʼ������10��
		//���Ҫ��60mb-120mb����ʼʱָ��seek(60*1024*1024) Ȼ��whileѭ������һ������������������120mb
		
	}
	
	/*
	 * ��ʾhdfs���ļ�������
	 */
	@Test
	public void testCat() throws IllegalArgumentException, IOException{
		FSDataInputStream in = f.open(new Path("/aa.txt"));
		org.apache.hadoop.io.IOUtils.copyBytes(in, System.out, 1024);//system.out��һ��printstream�����
	}

}
