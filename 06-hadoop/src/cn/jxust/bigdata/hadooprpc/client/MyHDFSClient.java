package cn.jxust.bigdata.hadooprpc.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import cn.jxust.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;

/*
 * hadooprpc的jar包在hadoop的common包下，可以用来自己使用，来实现远程rpc，如应用到web中action去远程调用serveimpl，但是web中用spring更好，spring会通过注解去发布服务（构建远程实例对象），而这里我们要手动去启动服务
 * (只要服务端namenode启动为服务后，客户端在哪个工程下写都可以，只要指定接口名即可)
 * hdfs的客户端，通过hadoop的rpc框架来获取一个动态代理对象
 * 通过代理对象去调用接口ClientNamenodeProtocol中的方法即可（RPC），就像本地调用一样
 */
public class MyHDFSClient {
	public static void main(String[] args) throws IOException {
		//返回一个远程rpc代理对象  
		//参数：遵循的协议，即客户端要调用的接口   协议的版本（在接口ClientNamenodeProtocol中指定的）   namenode的服务地址   环境配置
		ClientNamenodeProtocol namenode = RPC.getProxy(ClientNamenodeProtocol.class, 1L, new InetSocketAddress("localhost", 8888), new Configuration());
		//代理对象截取你要调用的方法、方法名，在invoke方法中通过socket（rpc）去调用namenode中接口中的方法，服务端namenode接收到请求后，根据传来的参数通过反射调用方法，然后把结果返回给代理对象，代理对象再把结果返回给我
		String metaData = namenode.getMetaData("/zzd");
		System.out.println(metaData);
	}
}
