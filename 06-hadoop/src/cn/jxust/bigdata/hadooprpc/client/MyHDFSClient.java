package cn.jxust.bigdata.hadooprpc.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import cn.jxust.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;

/*
 * hadooprpc��jar����hadoop��common���£����������Լ�ʹ�ã���ʵ��Զ��rpc����Ӧ�õ�web��actionȥԶ�̵���serveimpl������web����spring���ã�spring��ͨ��ע��ȥ�������񣨹���Զ��ʵ�����󣩣�����������Ҫ�ֶ�ȥ��������
 * (ֻҪ�����namenode����Ϊ����󣬿ͻ������ĸ�������д�����ԣ�ֻҪָ���ӿ�������)
 * hdfs�Ŀͻ��ˣ�ͨ��hadoop��rpc�������ȡһ����̬�������
 * ͨ���������ȥ���ýӿ�ClientNamenodeProtocol�еķ������ɣ�RPC�������񱾵ص���һ��
 */
public class MyHDFSClient {
	public static void main(String[] args) throws IOException {
		//����һ��Զ��rpc�������  
		//��������ѭ��Э�飬���ͻ���Ҫ���õĽӿ�   Э��İ汾���ڽӿ�ClientNamenodeProtocol��ָ���ģ�   namenode�ķ����ַ   ��������
		ClientNamenodeProtocol namenode = RPC.getProxy(ClientNamenodeProtocol.class, 1L, new InetSocketAddress("localhost", 8888), new Configuration());
		//��������ȡ��Ҫ���õķ���������������invoke������ͨ��socket��rpc��ȥ����namenode�нӿ��еķ����������namenode���յ�����󣬸��ݴ����Ĳ���ͨ��������÷�����Ȼ��ѽ�����ظ�������󣬴�������ٰѽ�����ظ���
		String metaData = namenode.getMetaData("/zzd");
		System.out.println(metaData);
	}
}
