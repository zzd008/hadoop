package cn.jxust.bigdata.hadooprpc.service;

import cn.jxust.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;

/*
 * 模拟namenode的业务方法
 */
public class Mynamenode implements ClientNamenodeProtocol{
	
	//模拟nanonode，返回文件的元数据
	@Override
	public String getMetaData(String path){
		return path+"：{blk1-blk-2}"+"...";
	}
	
}
