package cn.jxust.bigdata.logenhance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

public class DBLoader {
	//从数据库中查出所有的商品信息
	public static void dbLoader(Map<String,String> ruleMap) throws Exception{
		Connection conn = null;
		Statement st = null;
		ResultSet res = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
//			conn = DriverManager.getConnection("jdbc:mysql://192.168.110.133:3306/urldb", "root", ",.,.,,.."); //在集群中跑程序时，访问虚拟机上的数据库
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/urldb", "root", ",.,.,,..");
			
			String sql="select url,content from url_rule";//这里只查询出url和url的描绘信息
			PreparedStatement ps = conn.prepareStatement(sql);
			res = ps.executeQuery();
			while (res.next()) {
				ruleMap.put(res.getString(1), res.getString(2));//放到ruleMap中去
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
