package com.esclient;

import java.util.List;

import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilder;

import tlz.dm.conf.EsNode;

import com.google.common.base.Preconditions;


/**
 * es 查询帮助类
 *
 * @author chenfg
 */
public class EsDlsQuryHelper {

    private final Logger log = Logger.getLogger(EsDlsQuryHelper.class);

    private TransportClient transportClient;

    public EsDlsQuryHelper(String esClusterName, List<EsNode> clusterNode) {
        Preconditions.checkNotNull(esClusterName);
        Preconditions.checkNotNull(clusterNode);
        transportClient = new ElasticClientManager().getESTransportClient(esClusterName, clusterNode);
    }

    public SearchResponse queryCommon(String index, String type, SearchType searchType, QueryBuilder queryBuid, int from, int size, SortBuilder sortBuilder) {
        //QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index).setTypes(type).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(queryBuid) // Query
                .setPostFilter(queryBuid) // Filter
                .setFrom(from).setSize(size).addSort(sortBuilder).setExplain(true);
        SearchResponse searchRes = searchRequestBuilder.execute().actionGet();
        return searchRes;
    }

    /**
     * 词条查询
     *
     * @param name  字段名
     * @param value
     * @param index 索引
     * @param type  类型
     * @param from  开始序号
     * @param size  返回的结果条数
     * @return
     */
    public <T> SearchResponse termQuery(String name, T value, String index, String type, int from, int size) {
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);
        QueryBuilder qb = QueryBuilders.termQuery(name, value);
        SearchResponse searchRes = searchRequestBuilder.setQuery(qb)
                .setTypes(type)
                .setFrom(from)
                .setSize(size)
                .execute().actionGet();
        return searchRes;

    }

    /**
     * 词条查询
     *
     * @param name  字段名
     * @param value
     * @param index 索引
     * @param type  类型
     * @param from  开始序号
     * @param size  返回的结果条数
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> SearchResponse termsQuery(String name, String index, String type, int from, int size, T... values) {
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);
        QueryBuilder qb = QueryBuilders.termsQuery(name, values);
        SearchResponse searchRes = searchRequestBuilder.setQuery(qb)
                .setTypes(type)
                .setFrom(from)
                .setSize(size)
                .execute().actionGet();
        return searchRes;

    }

    /**
     * 常用词查询
     *
     * @param index
     * @param type
     * @param name
     * @param text
     * @param from
     * @param size
     * @return
     */
    public SearchResponse commonQuery(String index, String type, String name, String text, int from, int size) {
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);
        QueryBuilder qb = QueryBuilders.commonTermsQuery(name, text);
        SearchResponse searchRes = searchRequestBuilder.setQuery(qb)
                .setTypes(type)
                .setFrom(from)
                .setSize(size)
                .execute().actionGet();
        return searchRes;
    }

    /**
     * 查询index下的所有文档
     *
     * @param index
     * @param from
     * @param size
     * @return
     */
    public SearchResponse matchAllQuery(String index, int from, int size) {
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchResponse searchRes = searchRequestBuilder.setQuery(qb)
                .setFrom(from)
                .setSize(size)
                .execute().actionGet();
        return searchRes;
    }

    /**
     * match 查询  分词处理查询
     *
     * @param index
     * @param type
     * @param name
     * @param text
     * @param from
     * @param size
     * @return
     */
    public SearchResponse matchQuery(String index, String type, String name, String text, int from, int size) {
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);
        QueryBuilder qb = QueryBuilders.matchQuery(name, text);
        SearchResponse searchRes = searchRequestBuilder.setQuery(qb)
                .setTypes(type)
                .setFrom(from)
                .setSize(size)
                .execute().actionGet();
        return searchRes;
    }

    /**
     * bool 查询 not complete
     *
     * @param index
     * @param type
     * @param name
     * @param text
     * @param from
     * @param size
     * @return
     */
    public SearchResponse boolQuery(String index, String type, String name, String text, int from, int size) {
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);
        QueryBuilder qb = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery(name, text))
                .must(QueryBuilders.termQuery(name, text))
                .mustNot(QueryBuilders.termQuery(name, text))
                .should(QueryBuilders.termQuery(name, text));
        SearchResponse searchRes = searchRequestBuilder.setQuery(qb)
                .setTypes(type)
                .setFrom(from)
                .setSize(size)
                .execute().actionGet();
        return searchRes;
    }

    /**
     * multi match 匹配多个字段
     *
     * @param index
     * @param type
     * @param name
     * @param text
     * @param from
     * @param size
     * @param fileds
     * @return
     */
    public SearchResponse multiMatches(String index, String type, String name, String text, int from, int size, String... fileds) {
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);
        QueryBuilder qb = QueryBuilders.multiMatchQuery(text, fileds);
        SearchResponse searchRes = searchRequestBuilder.setQuery(qb)
                .setTypes(type)
                .setFrom(from)
                .setSize(size)
                .execute().actionGet();
        return searchRes;
    }

    /**
     * query String
     *
     * @param index
     * @param type
     * @param text
     * @param from
     * @param size
     * @return
     */
    public SearchResponse queryString(String index, String type, String text, int from, int size) {
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);
        QueryBuilder qb = QueryBuilders.queryStringQuery(text);
        SearchResponse searchRes = searchRequestBuilder.setQuery(qb)
                .setTypes(type)
                .setFrom(from)
                .setSize(size)
                .execute().actionGet();
        return searchRes;
    }

    /**
     * @param index
     * @param type
     * @param text
     * @param from
     * @param size
     * @return
     */
    public SearchResponse simpleQueryString(String index, String type, String text, int from, int size) {
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);
        QueryBuilder qb = QueryBuilders.simpleQueryStringQuery(text);
        SearchResponse searchRes = searchRequestBuilder.setQuery(qb)
                .setTypes(type)
                .setFrom(from)
                .setSize(size)
                .execute().actionGet();
        return searchRes;
    }

    //关闭transport client
    public void closeClient() {
        if (null != transportClient) {
            transportClient.close();
        }
    }

    public Logger getLog() {
        return log;
    }
}
