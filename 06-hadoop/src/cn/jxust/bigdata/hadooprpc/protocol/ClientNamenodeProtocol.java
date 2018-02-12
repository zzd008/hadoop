package cn.jxust.bigdata.hadooprpc.protocol;

/*客户端-namenode遵循的协议，即共同接口
 * namenode把ClientNamenodeProtocol的实现类启动为服务
 * 然后客户端通过rpc去调用ClientNamenodeProtocol中的方法，就像调用本地的方法一样（动态代理）
 */
public interface ClientNamenodeProtocol {
	public static final long versionID=1L;//指定协议（接口）的协议号，客户端可以和这个不一样，不会去验证，但是 字段定义一定要为versionID，不然会不识别
	public String getMetaData(String path);
}
