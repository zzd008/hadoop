package cn.jxust.bigdata.mr.weblogwash;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.junit.Test;

/*
 * ����������
 */
public class WebLogParaser {								
	private static SimpleDateFormat sd1=new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);//��־�в��õ���usʱ���ʽ  18/Sep/2013:06:49:42 2λ��/��λ��/��λ��..��֮��Ӧ
	private static SimpleDateFormat sd2=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	public static WebLogBean paraser(String line){
		WebLogBean bean=new WebLogBean();
		
		String[] fields = line.split(" ");
		
		//��������ݷָ��Ҫ����11��
		if(fields.length>11){
			bean.setRemote_addr(fields[0]);
			bean.setRemote_user(fields[1]);// -
			bean.setTime_local(parseTime(fields[3].substring(1)));
			bean.setRequest(fields[6]);
			bean.setStatus(fields[8]);
			bean.setBody_bytes_sent(fields[9]);
			bean.setHttp_referer(fields[10]);
			
			//����ֶγ��ȴ���12����ô����Ķ��㵽�û����������Ϣ��
			if (fields.length >12) {
				bean.setHttp_user_agent(fields[11] + " " + fields[12]);
			} else {
				bean.setHttp_user_agent(fields[11]);
			}
			
			if (Integer.parseInt(bean.getStatus()) >= 400) {// ����400��HTTP����,��־���ݲ��Ϸ�
				bean.setValued(false);
			}else {
				bean.setValued(true);
			}
		
		}
		return bean;
	}

	//��ʽ��ʱ��  ԭʼ��ʽ��18/Sep/2013:06:49:42
	public static String parseTime(String time) {
		Date date = null;
		try {
			date = sd1.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}//��ԭʼʱ�������date
		String timeString = sd2.format(date);//�������ɵ�date��ʽ����Ҫ�ĸ�ʽ
		return timeString;
	}
	
	@Test
	public void test(){
		System.out.println(WebLogParaser.parseTime("18/Sep/2013:06:49:42"));
	}
}
