package cn.jxust.bigdata.mr.weblogwash;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.junit.Test;

/*
 * 解析工具类
 */
public class WebLogParaser {								
	private static SimpleDateFormat sd1=new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);//日志中采用的是us时间格式  18/Sep/2013:06:49:42 2位日/三位月/四位年..与之对应
	private static SimpleDateFormat sd2=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	public static WebLogBean paraser(String line){
		WebLogBean bean=new WebLogBean();
		
		String[] fields = line.split(" ");
		
		//处理的数据分割后要大于11个
		if(fields.length>11){
			bean.setRemote_addr(fields[0]);
			bean.setRemote_user(fields[1]);// -
			bean.setTime_local(parseTime(fields[3].substring(1)));
			bean.setRequest(fields[6]);
			bean.setStatus(fields[8]);
			bean.setBody_bytes_sent(fields[9]);
			bean.setHttp_referer(fields[10]);
			
			//如果字段长度大于12，那么后面的都算到用户的浏览器信息里
			if (fields.length >12) {
				bean.setHttp_user_agent(fields[11] + " " + fields[12]);
			} else {
				bean.setHttp_user_agent(fields[11]);
			}
			
			if (Integer.parseInt(bean.getStatus()) >= 400) {// 大于400，HTTP错误,标志数据不合法
				bean.setValued(false);
			}else {
				bean.setValued(true);
			}
		
		}
		return bean;
	}

	//格式化时间  原始格式：18/Sep/2013:06:49:42
	public static String parseTime(String time) {
		Date date = null;
		try {
			date = sd1.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}//将原始时间解析成date
		String timeString = sd2.format(date);//将解析成的date格式成想要的格式
		return timeString;
	}
	
	@Test
	public void test(){
		System.out.println(WebLogParaser.parseTime("18/Sep/2013:06:49:42"));
	}
}
