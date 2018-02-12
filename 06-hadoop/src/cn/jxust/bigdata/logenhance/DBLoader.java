package cn.jxust.bigdata.logenhance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

public class DBLoader {
	//�����ݿ��в�����е���Ʒ��Ϣ
	public static void dbLoader(Map<String,String> ruleMap) throws Exception{
		Connection conn = null;
		Statement st = null;
		ResultSet res = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
//			conn = DriverManager.getConnection("jdbc:mysql://192.168.110.133:3306/urldb", "root", ",.,.,,.."); //�ڼ�Ⱥ���ܳ���ʱ������������ϵ����ݿ�
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/urldb", "root", ",.,.,,..");
			
			String sql="select url,content from url_rule";//����ֻ��ѯ��url��url�������Ϣ
			PreparedStatement ps = conn.prepareStatement(sql);
			res = ps.executeQuery();
			while (res.next()) {
				ruleMap.put(res.getString(1), res.getString(2));//�ŵ�ruleMap��ȥ
			}
			
		} finally {
			try{
				if(res!=null){
					res.close();
				}
				if(st!=null){
					st.close();
				}
				if(conn!=null){
					conn.close();
				}
	
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}
