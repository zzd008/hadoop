package cn.jxust.bigdata.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * �Զ�������� ��������� <order_001,99.9> <order_001,88.8> <order_001,77.7> ���ֵ�ͬһ��reduce
 * redcueҪ����ͬ��key�ۺϳ�һ��ȥ����������Щkey��orderbean���󶼲�ͬ�����ԾͲ��ᱻ�ۺϣ�ÿ���������������
 * reduce��Ĭ���Ǹ���key��ֵ�Ƿ���ͬ���ۺϵģ����ǿ����Զ���ۺϷ���
 */
public class OrderIdGroupingCompator extends WritableComparator{
	//ָ��key��class���ͣ�����keyȥ�Ƚ��Ƿ���ͬ������ͨ��������������һ������ 
	public OrderIdGroupingCompator() {
		super(OrderBean.class,true);
	}
	
	//
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean a1=(OrderBean)a;
		OrderBean b1=(OrderBean)b;
		//�Ƚ�����beanʱ��ָ��ֻ�Ƚ�bean�е�orderid
		return a1.getOrderId().compareTo(b1.getOrderId());//0��ͬ ȥ������ͬ
	}
}
