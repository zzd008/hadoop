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
 * �������
 * ��mapreduce ��spark���������У���һ������˼����ǽ������������ݣ�����˵������Ҫ�ڲ��������о����������㱾�ػ��������Ҫ��ȡ��������λ�õ���Ϣ��������Ӧ��Χ��ȡ
 * ����ģ��ʵ�֣���ȡһ���ļ�������blockλ����Ϣ��Ȼ���ȡָ��block�е�����
 */
public class ReadBlock {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		Configuration conf=new Configuration();
		FileSystem f=FileSystem.get(new URI("hdfs://master:9000"), conf, "hadoop");
		
		//�ϴ�test.txt��hdfs �ļ���Сû�б��п�
		f.copyFromLocalFile(new Path("C:/Users/zzd/Desktop/test.txt"), new Path("/"));
		
		FileStatus[] fileStatus = f.listStatus(new Path("/test.txt"));
		FileStatus fs=fileStatus[0];
		//��ȡ����Ϣ
		BlockLocation[] fileBlockLocations = f.getFileBlockLocations(fs, 0, fs.getLen());//Ҳ����f.listfile Ȼ��getBlockLocations liststatus������û���������
		System.out.println(fileBlockLocations.length);//��Ϊ�ļ���Сû�б��ֿ飬����Ϊ1
		
		BlockLocation bl = fileBlockLocations[0];
		//��Ϊû�б��ֿ飬������ʼƫ�����ͳ���Ϊ 0 26
		System.out.println(bl.getOffset()+" "+bl.getLength()+" ");
		
		//��ȡ5-20�����ݳ���
		FSDataInputStream in = f.open(new Path("/test.txt"));
		FileOutputStream out = new FileOutputStream("C:/Users/zzd/Desktop/test1.txt");
		
		//����һ
//		IOUtils.copyLarge(in, out, 5, 20-5);//������õķ��� ���ӵ�5����ʼ������20-5��
		
		//������
		/*in.seek(5);//�ӵ�����ֽڿ�ʼ��
		int len;
		int count=0;
		while((len=in.read())!=-1){ //in.read()��һ�Σ��������a ������arsicc�� 97
			System.out.println(len);
			out.write(len);//д��
			count+=1;
//			if(count>=5)
			if(count>15) return;
		}*/
		
		/*while((len=in.read(b))!=-1){ �������У���һ����ȫ����������b�У����ȳ�����20,���԰����鳤�ȴ�1024��СΪ5����һ��ֻ��һ���ֽ�
			out.write(b, 0, len);
			count+=len;
			if(count>20) return;
		}*/
		
		/*out.flush();
		out.close();
		in.close();*/
	}
}
