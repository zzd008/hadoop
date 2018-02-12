package cn.jxust.bigdata.hadooprpc.service;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

import cn.jxust.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;

/*
 * 启动模拟的namenode，将其发为服务，客户端就可以去调用了
 */
public class PublicServiceUtils {
	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
		//构建builder对象
		Builder builder = new RPC.Builder(new Configuration());
		//配置bulider
		builder.setBindAddress("localhost")//地址
		.setPort(8888)//端口
		.setProtocol(ClientNamenodeProtocol.class)//客户端和namenode遵循的协议，即他们的共同接口 
		.setInstance(new Mynamenode());//namenode的实例对象  之前自定义的rpc是通过spring去构建实例对象
		
		//namenode把ClientNamenodeProtocol的实现类启动为服务，然后客户端通过pc去调用ClientNamenodeProtocol中的方法，就像调用本地的方法一样（动态代理）
		//发布服务
		Server server = builder.build();
		server.start();
	}
}
