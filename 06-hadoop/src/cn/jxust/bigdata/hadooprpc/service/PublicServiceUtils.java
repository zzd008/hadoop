package cn.jxust.bigdata.hadooprpc.service;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

import cn.jxust.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;

/*
 * ����ģ���namenode�����䷢Ϊ���񣬿ͻ��˾Ϳ���ȥ������
 */
public class PublicServiceUtils {
	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
		//����builder����
		Builder builder = new RPC.Builder(new Configuration());
		//����bulider
		builder.setBindAddress("localhost")//��ַ
		.setPort(8888)//�˿�
		.setProtocol(ClientNamenodeProtocol.class)//�ͻ��˺�namenode��ѭ��Э�飬�����ǵĹ�ͬ�ӿ� 
		.setInstance(new Mynamenode());//namenode��ʵ������  ֮ǰ�Զ����rpc��ͨ��springȥ����ʵ������
		
		//namenode��ClientNamenodeProtocol��ʵ��������Ϊ����Ȼ��ͻ���ͨ��pcȥ����ClientNamenodeProtocol�еķ�����������ñ��صķ���һ������̬����
		//��������
		Server server = builder.build();
		server.start();
	}
}
