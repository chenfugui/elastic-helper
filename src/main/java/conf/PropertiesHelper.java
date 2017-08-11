package conf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;

/**
 * @author chenfg
 *         property配置文件读取
 */
public class PropertiesHelper {
    private Log log = LogFactory.getLog(PropertiesHelper.class);
    private InputStream inputStream = null;
    private Properties props = null;

    //初始化
    public PropertiesHelper(String propfileName) {
        try {
            inputStream = new FileInputStream(PropertiesHelper.getProjectPath() + "/" + propfileName);
        } catch (FileNotFoundException e) {
            log.error("外置配置文件找不到加载内置配置文件......");
            //inputStream = new FileInputStream(this.getClass().getResource("/").getPath()+propfileName);
            inputStream = this.getClass().getResourceAsStream("/" + propfileName);

        } catch (UnsupportedEncodingException e) {

            e.printStackTrace();
        }
        props = new Properties();
        try {
            props.load(inputStream);
        } catch (IOException e1) {
            e1.printStackTrace();
        } finally {
            closePropertiesFile();
        }

    }

    /**
     * 获取某个属性
     */
    public String getProperty(String key) {
        return props.getProperty(key);
    }

    /**
     * 获取所有属性，返回一个map,不常用
     * 可以试试props.putAll(t)
     */
    public Map<String, String> getAllProperty() {
        Map<String, String> map = new HashMap<String, String>();
        Iterator<String> enu = props.stringPropertyNames().iterator();
        while (enu.hasNext()) {
            String key = enu.next();
            String value = props.getProperty(key);
            map.put(key, value);
        }
        return map;
    }

    /**
     * 获取所有属性，返回一个map,不常用
     * 可以试试props.putAll(t)
     */
    public List<String> getAllPropertyValues() {
        List<String> parternList = new ArrayList<String>();
        Iterator<String> enu = props.stringPropertyNames().iterator();
        while (enu.hasNext()) {
            String key = enu.next();
            String value = props.getProperty(key);
            if (!parternList.contains(value)) {
                parternList.add(value);
            }
        }
        return parternList;
    }

    /**
     * 在控制台上打印出所有属性，调试时用。
     */
    public void printProperties() {
        props.list(System.out);
    }


    /**
     * 关闭配置文件
     */
    public void closePropertiesFile() {

        if (inputStream != null) {
            try {
                inputStream.close();
                inputStream = null;
            } catch (IOException e) {
                log.error("关闭配置文件流出错      !!!");
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }

    }


    /**
     * 获取工程所在目录
     *
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String getProjectPath() throws UnsupportedEncodingException {
        URL url = PropertiesHelper.class.getProtectionDomain().getCodeSource().getLocation();
        String filePath = URLDecoder.decode(url.getPath(), "UTF-8");
        if (filePath.endsWith(".jar"))
            filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
        return filePath;
    }
    /**
     * 获取工程所在目录
     *
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String getProjectRunJarPath()  {
        URL url = PropertiesHelper.class.getProtectionDomain().getCodeSource().getLocation();
        String filePath = null;
        try {
            filePath = URLDecoder.decode(url.getPath(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return filePath;
    }
}
