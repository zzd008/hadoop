package cn.jxust.bigdata.hadooprpc.service;

import cn.jxust.bigdata.hadooprpc.protocol.ClientNamenodeProtocol;

/*
 * ģ��namenode��ҵ�񷽷�
 */
public class Mynamenode implements ClientNamenodeProtocol{
	
	//ģ��nanonode�������ļ���Ԫ����
	@Override
	public String getMetaData(String path){
		return path+"��{blk1-blk-2}"+"...";
	}
	
}
