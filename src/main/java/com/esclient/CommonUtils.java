package com.esclient;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 常用的公共方法
 * @author chenfg
 *
 */
public class CommonUtils implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 判断对象是否为null
	 * @param obj
	 * @return
	 */
	public static  boolean isNullObject(Object obj)
	{
		if(null==obj){
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 * 读取外置json File文件
	 * @param filePath
	 * @return
	 */
	public static String readJsonStrFormFile(String filePath)
	{
		BufferedReader reader = null;
		String laststr = "";
		try{
			FileInputStream fileInputStream = new FileInputStream(filePath);
			InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
			reader = new BufferedReader(inputStreamReader);
			String tempString = null;
			while((tempString = reader.readLine()) != null){
			laststr += tempString;
			}
			reader.close();
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			if(reader != null){
				try {
				reader.close();
				} catch (IOException e) {
				e.printStackTrace();
				}
			}
		}
		return laststr;
	}
	/**
	 * 获取内部mappin json
	 * @param fileName
	 * @return
	 */
	public static String readJsonStrFormInnerFile(String fileName)
	{
		BufferedReader reader = null;
		String laststr = "";
		try{
			//FileInputStream fileInputStream = new FileInputStream(filePath);
			InputStreamReader inputStreamReader = new InputStreamReader(CommonUtils.class.getResourceAsStream("/"+fileName));
			reader = new BufferedReader(inputStreamReader);
			String tempString = null;
			while((tempString = reader.readLine()) != null){
			laststr += tempString;
			}
			reader.close();
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			if(reader != null){
				try {
				reader.close();
				} catch (IOException e) {
				e.printStackTrace();
				}
			}
		}
		return laststr;
	}
	/**
	 * 判断字符串是否是数字
	 * @param str
	 * @return
	 */
	public static boolean isNumeric(String str){ 
		   Pattern pattern = Pattern.compile("[0-9]*"); 
		   Matcher isNum = pattern.matcher(str);
		   if( !isNum.matches() ){
		       return false; 
		   } 
		   return true; 
		}
}
