package com.esclient;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author chenfg
 *         es 帮助类
 */
public class ElasticHelper {

    private final Logger log = Logger.getLogger(ElasticHelper.class);

    private TransportClient transportClient;

    public static final int bulkCommitSize = 10000;

    /**
     * @param esClusterName
     * @param clusterNodeAndPortMap
     */
    public ElasticHelper(String esClusterName, Map<String,Integer> clusterNodeAndPortMap) {
        Preconditions.checkNotNull(esClusterName);
        Preconditions.checkNotNull(clusterNodeAndPortMap);
        transportClient = new ElasticClientManager().getESTransportClient(esClusterName, clusterNodeAndPortMap);
    }

    public ElasticHelper(String esClusterName, Map<String,Integer> clusterNodeAndPortMap, Map<String, String> propertyMap) {
        Preconditions.checkNotNull(esClusterName);
        Preconditions.checkNotNull(clusterNodeAndPortMap);
        transportClient = new ElasticClientManager().getESTransportClient(esClusterName, clusterNodeAndPortMap);
    }

    /**
     * 创建index type和type mapping
     *
     * @param indexName
     * @param type
     * @param mappingJson
     * @return
     */
    public PutMappingResponse createIndexByMapping(String indexName, String type, String mappingJson) {
        IndicesExistsRequest request = new IndicesExistsRequest().indices(new String[]{indexName});
        if (!transportClient.admin().indices().exists(request).actionGet().isExists()) {
            transportClient.admin().indices().prepareCreate(indexName).execute().actionGet();
        }
        TypesExistsRequest typeReq = new TypesExistsRequest(new String[]{indexName}, type);
        if (!transportClient.admin().indices().typesExists(typeReq).actionGet().isExists()) {
            return transportClient.admin().indices().preparePutMapping(indexName).setType(type).setSource(mappingJson).execute().actionGet();
        }
        return null;
    }

    /**
     * 创建index type和type mapping
     *
     * @param indexName
     * @param type
     * @param mappingJsonFile
     * @return
     */
    public PutMappingResponse createIndexByMappingJsonFile(String indexName, String type, String mappingJsonFile) {
        String jsonMap = CommonUtils.readJsonStrFormInnerFile(mappingJsonFile);
        //String jsonMap=CommonUtils.readJsonStrFormFile(EsConfigHelper.getJsonFileName(mappingJsonFile));
        IndicesExistsRequest request = new IndicesExistsRequest().indices(new String[]{indexName});
        if (!transportClient.admin().indices().exists(request).actionGet().isExists()) {
            transportClient.admin().indices().prepareCreate(indexName).execute().actionGet();
        }
        TypesExistsRequest typeReq = new TypesExistsRequest(new String[]{indexName}, type);
        if (!transportClient.admin().indices().typesExists(typeReq).actionGet().isExists()) {
            return transportClient.admin().indices().preparePutMapping(indexName).setType(type).setSource(jsonMap).execute().actionGet();
        }
        return null;
    }

    /**
     * 创建索引
     *
     * @param index
     * @param obj
     * @param docId
     * @return
     */
    public IndexResponse createIndex(String index, String type, Object obj, String docId) {
        String objson = JSON.toJSONString(obj);
        return transportClient.prepareIndex(index, type, docId).setSource(objson).get();
    }


    /**
     * 创建索引
     *
     * @param index
     * @param obj
     * @return
     */
    public IndexResponse createIndex(String index, String type, Object obj) {
        String objson = JSON.toJSONString(obj);
        return transportClient.prepareIndex(index, type).setSource(objson).get();
    }

    /**
     * 获取索引文档
     *
     * @param index
     * @param type
     * @param docId
     * @return
     */
    public GetResponse getIndexDoc(String index, String type, String docId) {
        GetResponse response = transportClient.prepareGet(index, type, docId)
                .setOperationThreaded(false)
                .get();
        return response;
    }

    /**
     * 删除文档
     *
     * @param index
     * @param type
     * @param docId
     * @return
     */
    public DeleteResponse delIndexedDoc(String index, String type, String docId) {

        return transportClient.prepareDelete(index, type, docId).get();
    }

    /**
     * 删除索引
     *
     * @param index
     * @return
     */
    public DeleteIndexResponse delIndex(String index) {

        return transportClient.admin().indices().prepareDelete(index).execute().actionGet();
    }

    /**
     * 更新文档  simple recursive merge merged into the existing document
     *
     * @param index
     * @param type
     * @param docId
     * @param obj
     * @return
     */
    public UpdateResponse updateByMergDoc(String index, String type, String docId, Object obj) {
        String json = JSON.toJSONString(obj);
        UpdateRequest updateRequest = new UpdateRequest(index, type, docId).doc(json);
        UpdateResponse uresponse = null;
        try {
            uresponse = transportClient.update(updateRequest).get();
        } catch (InterruptedException e) {
            log.error("update by merge doc " + docId + " failed ,please check !!!");
            log.error(e.getMessage());
            e.printStackTrace();
        } catch (ExecutionException e) {
            log.error("update by merge doc " + docId + " failed ,please check !!!");
            log.error(e.getMessage());
            e.printStackTrace();
        }
        return uresponse;
    }

    /**
     * update by sert
     *
     * @param index
     * @param type
     * @param docId
     * @param docObj    document
     * @param modifyObj 修改的属性
     * @return
     */
    public UpdateResponse updateSertDoc(String index, String type, String docId, Object docObj, Object modifyObj) {
        UpdateResponse uresponse = null;
        String docJson = JSON.toJSONString(docObj);
        IndexRequest indexRequest = new IndexRequest(index, type, docId)
                .source(docJson);
        String modeJson = JSON.toJSONString(modifyObj);
        UpdateRequest updateRequest = new UpdateRequest(index, type, docId)
                .doc(modeJson)
                .upsert(indexRequest);
        try {
            uresponse = transportClient.update(updateRequest).get();
        } catch (InterruptedException e) {
            log.error("update doc " + docId + " failed ,please check !!!");
            log.error(e.getMessage());
            e.printStackTrace();
        } catch (ExecutionException e) {
            log.error("update doc " + docId + " failed ,please check !!!");
            log.error(e.getMessage());
            e.printStackTrace();
        }
        return uresponse;
    }

    /**
     * 获取某个索引 type 下的多个文档
     *
     * @param index
     * @param type
     * @param queryIds
     * @return
     */
    public List<String> multiGetItem(String index, String type, Collection<String> queryIds) {
        MultiGetResponse multiGetItemResponses = transportClient.prepareMultiGet()
                .add(index, type, queryIds)
                .get();
        List<String> lst = new ArrayList<String>();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {

                lst.add(response.getSourceAsString());
            }
        }
        return lst;
    }

    /**
     * 获取Collection<EsItemsVo> 中的多个item文档
     *
     * @param queryItems
     * @return
     */
    public List<String> multiGetItem(Collection<EsItemsVo> queryItems) {
        MultiGetRequestBuilder multiGetBulder = transportClient.prepareMultiGet();
        Iterator<EsItemsVo> itrator = queryItems.iterator();
        while (itrator.hasNext()) {
            EsItemsVo itemvo = itrator.next();
            multiGetBulder.add(itemvo.getIndex(), itemvo.getType(), itemvo.getIdsCollect());
        }
        MultiGetResponse multiGetItemResponses = multiGetBulder.get();
        List<String> lst = new ArrayList<String>();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {

                lst.add(response.getSourceAsString());
            }
        }
        return lst;
    }

    /**
     * 获取bulkBuilder
     *
     * @return
     */
    public BulkRequestBuilder getBulkRequest() {
        BulkRequestBuilder bulkRequest = transportClient.prepareBulk();

        return bulkRequest;
    }


    /**
     * 构建IndexRequestBuilder 用于索引创建
     *
     * @param index
     * @param type
     * @param id
     * @param docJson
     * @return
     */
    public IndexRequestBuilder createIndexRequest(String index, String type, String id, String docJson) {

        if (StringUtils.isNotEmpty(id)) {
            return transportClient.prepareIndex(index, type, id).setSource(docJson);
        } else {
            return transportClient.prepareIndex(index, type).setSource(docJson);
        }
    }

    /**
     * 批量创建索引
     *
     * @param requestList
     * @return 全部创建成功返回true ,有一个创建失败则返回false
     */
    public boolean bulkRequestCommit(List<IndexRequestBuilder> requestList) {
        boolean flag = true;
        BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
        for (IndexRequestBuilder t : requestList) {
            bulkRequest.add(t);
        }
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {

            BulkItemResponse[] bulkItemArry = bulkResponse.getItems();
            for (BulkItemResponse item : bulkItemArry) {
                if (item.isFailed()) {
                    log.error(item.getFailureMessage());
                    log.warn(item.toString());
                    flag = false;
                }
            }
        }
        return flag;
    }

    /**
     * 批量由格式化的log字符串 批量创建同一类型的索引
     *
     * @param index
     * @param type
     * @param id
     * @param docJsonList doc位格式化过后的log数据
     * @return
     */
    public boolean bulkRequestCommitWithLogStr(String index, String type, String id, List<String> docJsonList) {
        boolean flag = true;
        //LogExtractService logExtS=new LogDataExtracter();
        BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
        for (String docStr : docJsonList) {
            if (null != docStr)
                bulkRequest.add(createIndexRequest(index, type, id, docStr));
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            BulkItemResponse[] bulkItemArry = bulkResponse.getItems();
            for (BulkItemResponse item : bulkItemArry) {
                if (item.isFailed()) {
                    log.error(item.getFailureMessage());
                    log.warn(item.toString());
                    flag = false;
                }
            }
        }

        return flag;
    }

    /**
     * 批量创建同一类型的索引
     *
     * @param index
     * @param type
     * @param id
     * @param docJsonList
     * @return
     */
    public boolean bulkRequestCommit(String index, String type, String id, List<String> docJsonList) {
        boolean flag = true;
        BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
        for (String docJson : docJsonList) {
            bulkRequest.add(createIndexRequest(index, type, id, docJson));
        }
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {

            BulkItemResponse[] bulkItemArry = bulkResponse.getItems();
            for (BulkItemResponse item : bulkItemArry) {
                if (item.isFailed()) {
                    log.error(item.getFailureMessage());
                    log.warn(item.toString());
                    flag = false;
                }
            }
        }
        return flag;
    }

    /**
     * 获取BulkProcessor 对象
     *
     * @param listener process监听对象
     * @return
     */
    public BulkProcessor getBulkProcess(Listener listener) {
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                transportClient, listener)
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
		        /*.setBackoffPolicy(
		            BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) */
                .build();
        return bulkProcessor;
    }

    /**
     * 批量创建索引 根据log文档数据
     *
     * @param index
     * @param type
     * @param docs
     * @return
     */
    public boolean createLogIndexForSpark(String index, String type, Iterator<String> docs) {
        List<String> docList = new ArrayList<String>();
        while (docs.hasNext()) {
            docList.add(docs.next());
            if (docList.size() >= bulkCommitSize) {
                bulkRequestCommit(index, type, null, docList);
                docList.clear();
            }
        }

        return true;
    }

    //关闭transport client
    public void closeClient() {
        if (null != transportClient) {
            transportClient.close();
        }
    }

    /**
     * 根据mapping jsonFile 创建index的mapping
     *
     * @param indexName
     * @param indexType
     * @param mappingJsonFile
     */
    public static void createIndexMapping(String indexName, String indexType, String mappingJsonFile) {
        ElasticHelper eshelperCreateMappig = new ElasticHelper(ElasticClientManager.esClusterName, ElasticClientManager.esNodesMap);
        eshelperCreateMappig.createIndexByMappingJsonFile(indexName, indexType, mappingJsonFile);
        eshelperCreateMappig.closeClient();
    }
}
