package cn.jxust.bigdata.mr.weblogwash;

/*
 * 存放日志中每一行数据中的字段 因为清洗流程不需要reduce，所以不用实现hadoop的序列化接口writable
 */
public class WebLogBean {
	private String remote_addr;// 客户端的ip地址
    private String remote_user;// 客户端用户名称,忽略属性"-"，日志中没有写，用--表示
    private String time_local;// 访问时间与时区  日志中使用的是licale.us时间格式
    private String request;// 请求的url与http协议
    private String status;// 请求状态码 ，成功是200
    private String body_bytes_sent;// 发送给客户端文件主体内容大小
    private String http_referer;// 从那个页面链接访问过来的
    private String http_user_agent;// 客户浏览器的相关信息

    private boolean isValued;//标志数据是否合法
    
	public boolean isValued() {
		return isValued;
	}
	public void setValued(boolean isValued) {
		this.isValued = isValued;
	}
	public String getRemote_addr() {
		return remote_addr;
	}
	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}
	public String getRemote_user() {
		return remote_user;
	}
	public void setRemote_user(String remote_user) {
		this.remote_user = remote_user;
	}
	public String getTime_local() {
		return time_local;
	}
	public void setTime_local(String time_local) {
		this.time_local = time_local;
	}
	public String getRequest() {
		return request;
	}
	public void setRequest(String request) {
		this.request = request;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getBody_bytes_sent() {
		return body_bytes_sent;
	}
	public void setBody_bytes_sent(String body_bytes_sent) {
		this.body_bytes_sent = body_bytes_sent;
	}
	public String getHttp_referer() {
		return http_referer;
	}
	public void setHttp_referer(String http_referer) {
		this.http_referer = http_referer;
	}
	public String getHttp_user_agent() {
		return http_user_agent;
	}
	public void setHttp_user_agent(String http_user_agent) {
		this.http_user_agent = http_user_agent;
	}
	@Override
	public String toString() {
		StringBuffer sb=new StringBuffer();
		sb.append(this.isValued);
		// \001是一种不可见的二进制字符，可以避免与内容连在一起
        sb.append("\001").append(this.remote_addr);
        sb.append("\001").append(this.remote_user);
        sb.append("\001").append(this.time_local);
        sb.append("\001").append(this.request);
        sb.append("\001").append(this.status);
        sb.append("\001").append(this.body_bytes_sent);
        sb.append("\001").append(this.http_referer);
        sb.append("\001").append(this.http_user_agent);
		return sb.toString();
	}
    
    
}
