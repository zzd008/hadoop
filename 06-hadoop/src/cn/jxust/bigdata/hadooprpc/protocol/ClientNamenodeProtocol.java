package cn.jxust.bigdata.hadooprpc.protocol;

/*�ͻ���-namenode��ѭ��Э�飬����ͬ�ӿ�
 * namenode��ClientNamenodeProtocol��ʵ��������Ϊ����
 * Ȼ��ͻ���ͨ��rpcȥ����ClientNamenodeProtocol�еķ�����������ñ��صķ���һ������̬����
 */
public interface ClientNamenodeProtocol {
	public static final long versionID=1L;//ָ��Э�飨�ӿڣ���Э��ţ��ͻ��˿��Ժ������һ��������ȥ��֤������ �ֶζ���һ��ҪΪversionID����Ȼ�᲻ʶ��
	public String getMetaData(String path);
}
