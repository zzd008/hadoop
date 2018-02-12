package cn.jxust.bigdata.mr.provinceflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

import cn.jxust.bigdata.mr.flowsum.FlowBean;

/*
 * 自定义分区，默认是采用HashPartitioner
 * 老的api是实现hadoop.mapred.Partitioner接口，这里面有两个方法
 * 新的api是继承hadoop.mapreduce.Partitioner
 * 
 * hdfs上的输出文件和区号是对应的，分区1就叫part-001 2就是part-002
 * 
 * 分省统计用户的上下行总流量，这时就不能用hashcode的，如果分5个省，就要指定5个reducetask，每个reducetask负责汇总一个省的，map的逻辑和reduce逻辑不用改变
 * 
 * k2 v2 是map的输出类型，map的context.write()后，会有outputcollertor来收集，调用getPartition()方法来进行分区，并排序，如果缓存不够就溢写到磁盘上，归并。。。。(shuffle)
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean>{
	public static Map<String,Integer> provinceDict=new HashMap<String,Integer>();//号码归属地
	
	//让类一加载时就把号码归属地hashmap加载到内存中，这样效率很高
	static{
		
		provinceDict.put("136", 0);//北京
		provinceDict.put("137", 1);//上海
		provinceDict.put("138", 2);//天津
		provinceDict.put("139", 3);//湖北
		//其他的都归为台湾
	}

	/*
	 * 这里分成几个区reducetask数量要大于等于分的区数
	 *  传入一个key value就调用一次getPartition
	 *  返回值一共有5个整数0 1 2 3 4 所以就分成五个区，所以要指定5个reducetask
	 *	hdfs上的输出文件和区号是对应的，返回区号1就叫part-001 2就是part-002	 *  
	 */
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		String subPhone=key.toString().substring(0, 3);
		Integer provinceId=provinceDict.get(subPhone);
		return provinceId==null?4:provinceId;//其他的都归为台湾
	}

}
