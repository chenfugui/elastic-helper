package com.esclient;

import conf.EsConfigHelper;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * @author chenfg
 *         elastic search client 管理类
 */
public class ElasticClientManager {
    //es集群名称
    public static final String esClusterName = new EsConfigHelper().getEsClusterName();
    //es集群节点
    public static final Map<String,Integer> esNodesMap = new EsConfigHelper().getClusterEsNodes();
    //log index名称
    public static final String logIndexName = "weblog";
    //log索引mapping json文件
    public static final String logIndexModelFile = "weblog.json";

    /**
     * 获取es transport client
     * @return
     *//*
    public TransportClient getESTransportClient()
	{
		EsConfigHelper eshelper=new EsConfigHelper();
		List<EsNode> nodeLst=eshelper.getClusterEsNodes();
		if(null!=nodeLst&&nodeLst.size()>0)
		{
			Settings settings = Settings.settingsBuilder()
					.put("cluster.name", eshelper.getEsClusterName()).build();
			TransportClient client = TransportClient.builder().settings(settings).build();
			for(EsNode node:nodeLst)
			{
				
				try {
					
					client.addTransportAddress(
							new InetSocketTransportAddress(InetAddress.getByName(node.getHostName()), node.getPort()));
					
				} catch (UnknownHostException e) {
					
					e.printStackTrace();
					return null;
				}
				
			}
			return client;
		}
		
		return null;
	}*/

    /**
     * 根据指定的es集群名称和esnode列表获取 transport client
     * 获取es transport client
     *
     * @return
     */
    public TransportClient getESTransportClient(String clusterName, Map<String,Integer> nodeMap) {


        if (null != nodeMap && nodeMap.size() > 0) {
            Settings settings = Settings.builder()
                    .put("cluster.name", clusterName)
                    .build();
            TransportClient client = new PreBuiltTransportClient(settings);
            for (Map.Entry<String,Integer> enty : nodeMap.entrySet()) {

                try {

                    client.addTransportAddress(
                            new InetSocketTransportAddress(InetAddress.getByName(enty.getKey()), enty.getValue()));

                } catch (UnknownHostException e) {

                    e.printStackTrace();
                    return null;
                }

            }
            return client;
        }

        return null;
    }

    /**
     * 根据指定的es集群名称和esnode列表获取 transport client
     * 获取es transport client
     *
     * @return
     */
    public TransportClient getBulkESTransportClient(String clusterName, Map<String,Integer> nodeMap, Map<String, String> propertyMap) {


        if (null != nodeMap && nodeMap.size() > 0) {
            Builder builder = Settings.builder()
                    .put("cluster.name", clusterName);
            if (null != propertyMap)
                for (String keyStr : propertyMap.keySet()) {
                    builder.put(keyStr, propertyMap.get(keyStr));
                }
            Settings settings = builder.build();
            TransportClient client = new PreBuiltTransportClient(settings);
            for (Map.Entry<String,Integer> enty : nodeMap.entrySet()) {

                try {

                    client.addTransportAddress(
                            new InetSocketTransportAddress(InetAddress.getByName(enty.getKey()), enty.getValue()));

                } catch (UnknownHostException e) {

                    e.printStackTrace();
                    return null;
                }

            }
            return client;
        }

        return null;
    }

    /**
     * 关闭transport client
     *
     * @param client
     */
    public void closeTransportClient(TransportClient client) {
        if (null != client) {
            client.close();
        }
    }

}
