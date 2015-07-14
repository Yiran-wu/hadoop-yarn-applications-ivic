package org.apache.hadoop.yarn.applications.ivic;

import java.io.*;
import java.sql.*;
import java.util.*;

public class ConnectDataBase {
	private static String driver = "com.mysql.jdbc.Driver";
	private static String url = "jdbc:mysql://localhost:3306/iVIC_Portal_development";
	private static String username = "root";
	private static String password = "123456";
	private Connection con = null;
	private PreparedStatement pstmt = null;//利用PreparedStatement来替代Statement防止sql注入
	private ResultSet rs = null;
	
	public ConnectDataBase() {
		// 利用构造方法加载JDBC驱动,注册数据库驱动程序
		/*Properties props = getProperties();
		if (props != null) {
			driver = props.getProperty("driver");
			url = props.getProperty("url");
			username = props.getProperty("user");
			password = props.getProperty("password");
		}*/
		try {
			Class.forName(driver).newInstance();
		} catch (Exception e) {
			System.out.println("数据库驱动加载失败！"); 
			e.printStackTrace();
		}
	}
	
	/**
	 * 建立数据库的连接
	 * @param 
	 * @return Connection
	 * @throws SQLException
	 */
	public Connection getDBConnection() {
		try {
			con = DriverManager.getConnection(url, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return con;
	}
	
	public static Properties getProperties() {
		Properties props = new Properties();
		InputStream in = null ;
		try {
			// 按照参数路路径获得属性文件构造文件输入流
			//in = ConnectDataBase.class.getResourceAsStream("../jdbc_mysql.properties");
			String path = "./jdbc_mysql.properties";
			System.out.println(path);
			System.out.println("111");
			System.out.println(Thread.currentThread().getContextClassLoader().getResource(""));
			System.out.println(ConnectDataBase.class.getClassLoader().getResource("")); 
			//String path = Thread.currentThread().getContextClassLoader().getResource("jdbc_mysql.properties").getPath();
			
			//System.out.println(path);
			//props.load(in); // 从输入流中读取属性表
			//String path = "/home/kongshuai/backup-hadoop-2.6.0-src/hadoop-2.6.0-src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-ivic/src/main/java/org/apache/hadoop/yarn/applications/ivic/jdbc_mysql.properties";
			props.load(new FileInputStream(path));
			//props.load(new FileInputStream(path));
			System.out.println("222");
		} catch (Exception e) {
			System.out.println("333");
			System.out.println(e.getMessage() + e.toString());
			e.printStackTrace();
	        return null;
	    }
	    finally {
	    	if (in != null) {
	    		try {
	    			in.close();
	    		} catch (IOException e) {
	    			e.printStackTrace();     
	    		}
	    	}
	    }
	    return props;
	}
	
	/**
	 * 获取查询结果集
	 * @param sql
	 * @return ResultSet
	 * @throws SQLException
	 */
	public ResultSet executeQuery(String sql){
        try{
        	con = this.getDBConnection();
        	pstmt = con.prepareStatement(sql);
        	/*
        	if(params != null) {
                for(int i = 0;i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }                    
            }*/
        	rs = pstmt.executeQuery();  
        }catch(Exception e){
        	e.printStackTrace();
        }
        return rs;
	}
	
	/**
	 * 执行一条insert,update和delete的操作
	 * @param sql
	 * @return boolean
	 * @throws SQLException
	 */
	public void executeUpdate(String sql) { 
        try {
        	con = this.getDBConnection();
        	pstmt = con.prepareStatement(sql);
        	pstmt.executeUpdate();  
        }catch(Exception e){
        	e.printStackTrace();
        	throw new RuntimeException(e.getMessage());
        }
    }
	
	/**
     * 执行多条insert,update和delete的操作
     * @param List<String> sqlList
     * @return boolean
     * @throws SQLException
     */
    public void executeUpdates(List<String> sqlList) { 
        try {
            con = this.getDBConnection();
            con.setAutoCommit(false);
            for(int i = 0; i < sqlList.size(); i++){
                pstmt = con.prepareStatement(sqlList.get(i));
                pstmt.executeUpdate();
            }
            con.commit();
        }catch(Exception e){
            e.printStackTrace();
            // 回滚
            try {
                con.rollback();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            
            //抛出异常，抛出运行异常，可以给调用该函数的函数一个选择
            //可以处理，也可以放弃处理
            throw new RuntimeException(e.getMessage());
        }
    }
}
