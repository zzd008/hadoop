package cn.jxust.bigdata.mr.weblogwash;

/*
 * �����־��ÿһ�������е��ֶ� ��Ϊ��ϴ���̲���Ҫreduce�����Բ���ʵ��hadoop�����л��ӿ�writable
 */
public class WebLogBean {
	private String remote_addr;// �ͻ��˵�ip��ַ
    private String remote_user;// �ͻ����û�����,��������"-"����־��û��д����--��ʾ
    private String time_local;// ����ʱ����ʱ��  ��־��ʹ�õ���licale.usʱ���ʽ
    private String request;// �����url��httpЭ��
    private String status;// ����״̬�� ���ɹ���200
    private String body_bytes_sent;// ���͸��ͻ����ļ��������ݴ�С
    private String http_referer;// ���Ǹ�ҳ�����ӷ��ʹ�����
    private String http_user_agent;// �ͻ�������������Ϣ

    private boolean isValued;//��־�����Ƿ�Ϸ�
    
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
		// \001��һ�ֲ��ɼ��Ķ������ַ������Ա�������������һ��
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
