package cn.jxust.bigdata.mr.provinceflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

import cn.jxust.bigdata.mr.flowsum.FlowBean;

/*
 * �Զ��������Ĭ���ǲ���HashPartitioner
 * �ϵ�api��ʵ��hadoop.mapred.Partitioner�ӿڣ�����������������
 * �µ�api�Ǽ̳�hadoop.mapreduce.Partitioner
 * 
 * hdfs�ϵ�����ļ��������Ƕ�Ӧ�ģ�����1�ͽ�part-001 2����part-002
 * 
 * ��ʡͳ���û�������������������ʱ�Ͳ�����hashcode�ģ������5��ʡ����Ҫָ��5��reducetask��ÿ��reducetask�������һ��ʡ�ģ�map���߼���reduce�߼����øı�
 * 
 * k2 v2 ��map��������ͣ�map��context.write()�󣬻���outputcollertor���ռ�������getPartition()���������з�����������������治������д�������ϣ��鲢��������(shuffle)
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean>{
	public static Map<String,Integer> provinceDict=new HashMap<String,Integer>();//���������
	
	//����һ����ʱ�ͰѺ��������hashmap���ص��ڴ��У�����Ч�ʺܸ�
	static{
		
		provinceDict.put("136", 0);//����
		provinceDict.put("137", 1);//�Ϻ�
		provinceDict.put("138", 2);//���
		provinceDict.put("139", 3);//����
		//�����Ķ���Ϊ̨��
	}

	/*
	 * ����ֳɼ�����reducetask����Ҫ���ڵ��ڷֵ�����
	 *  ����һ��key value�͵���һ��getPartition
	 *  ����ֵһ����5������0 1 2 3 4 ���Ծͷֳ������������Ҫָ��5��reducetask
	 *	hdfs�ϵ�����ļ��������Ƕ�Ӧ�ģ���������1�ͽ�part-001 2����part-002	 *  
	 */
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		String subPhone=key.toString().substring(0, 3);
		Integer provinceId=provinceDict.get(subPhone);
		return provinceId==null?4:provinceId;//�����Ķ���Ϊ̨��
	}

}
