package conf;

import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author chenfg
 * es配置帮助类
 */
public class EsConfigHelper implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private static final Logger log = Logger.getLogger(EsConfigHelper.class);

    private XMLConfigHelper xmlConfHelper = new XMLConfigHelper("elastic_conf.xml");


    /**
     * 获取es cluster name。
     * @return str
     */
    public String getEsClusterName() {

        return xmlConfHelper.getStringProperty("cluster.name");
    }

    /**
     * 根据配置文件获取es集群机器。
     *
     * @return
     */
    public Map<String,Integer> getClusterEsNodes() {
        XMLConfiguration xmlConf = xmlConfHelper.getXmlConfiguration();
        //System.out.println("get hostname====: "+xmlConf.getString("test"));
        Map<String,Integer> esnodeMap = new HashMap<String,Integer>();
        NodeList nodeLst = xmlConf.getDocument().getElementsByTagName("servicenode");
        for (int i = 0; i < nodeLst.getLength(); i++) {
            //System.out.println(i+"==========nodeName======"+nodeLst.item(i).getNodeName());
            // 获得元素，将节点强制转换为元素
            Element element = (Element) nodeLst.item(i);
            // 此时element就是一个具体的元素
            // 获取子元素：子元素hostname只有一个节点，之后通过getNodeValue方法获取节点的值
            String hostName = element.getElementsByTagName("hostname").item(0).getTextContent();
            //System.out.println("hostname: "+hostName);
            // 此处打印出为null
            // 因为节点getNodeValue的值永远为null
            // 解决方法：加上getFirstChild()
            String port = element.getElementsByTagName("port").item(0).getTextContent();
            //System.out.println("port: " + port);// 此处打印出书名
            if (StringUtils.isNotBlank(hostName) && StringUtils.isNotBlank(port) && StringUtils.isNumeric(port)) {
                esnodeMap.put(hostName, Integer.parseInt(port));
            }
        }
        return esnodeMap;
    }

    /**
     * 读取文件中的Json字符串。
     * @param fileName  文件名
     * @return string      全路径
     */
    public static String getJsonFileName(String fileName) {
        try {
            return XMLConfigHelper.getProjectPath() + fileName;
        } catch (UnsupportedEncodingException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

}
