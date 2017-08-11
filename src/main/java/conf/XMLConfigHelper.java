package conf;

import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.List;

public class XMLConfigHelper implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private static final Log log = LogFactory.getLog(XMLConfigHelper.class);
	private  InputStream inputStream = null;
	private  XMLConfiguration  xmlConf=null;
	private  String encode="utf-8";
	//初始化
	public XMLConfigHelper(String configFile)
	{
		Parameters params = new Parameters();
		FileBasedConfigurationBuilder<XMLConfiguration> builder =
				new FileBasedConfigurationBuilder<XMLConfiguration>(XMLConfiguration.class)
						.configure(params.xml()
								.setFileName(configFile)
								.setValidating(true));
		try {
			xmlConf = builder.getConfiguration();
		}catch (org.apache.commons.configuration2.ex.ConfigurationException e) {
			e.printStackTrace();
		} finally{
			closeXmlPropertiesFile();
		}

	}

	/**
	 * 获取某个属性
	 */
	public  String getStringProperty(String key){
		return xmlConf.getString(key);
	}


	/**
	 * 关闭配置文件
	 */
	public  void closeXmlPropertiesFile(){

		if (inputStream!=null){
			try {
				inputStream.close();
				inputStream=null;
			} catch (IOException e) {
				log.error("关闭配置文件流出错      !!!");
				log.error(e.getMessage());
				e.printStackTrace();
			}
		}

	}
	/**
	 * 返回xml配置对象
	 * @return
	 */
	public XMLConfiguration getXmlConfiguration()
	{

		return xmlConf;
	}
	/**
	 * 获取工程所在目录
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static String getProjectPath() throws UnsupportedEncodingException{
		URL url = XMLConfigHelper.class.getProtectionDomain().getCodeSource().getLocation();
		String filePath = URLDecoder.decode(url.getPath(), "UTF-8");
		if(filePath.endsWith(".jar"))
			filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
		log.info(filePath);
		return filePath;
	}

	/**
	 * 返回xml配置对象
	 * @return
	 */
	public List<Object> getList(String key)
	{
		List<Object> list=xmlConf.getList(key);
		for(Object obj:list){
			System.out.println(obj);
		}
		return list;
	}
}
