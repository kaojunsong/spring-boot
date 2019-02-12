package com.example.demo;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.ElasticsearchException;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentEntity;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentProperty;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/")
public class GoodsController {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @RequestMapping("save")
    public String save() {

        for (int i = 0; i < 100000; i++) {
            GoodsInfo goodsInfo = new GoodsInfo(System.currentTimeMillis(),
                    "商品" + System.currentTimeMillis(), "这是一个测试商品");

            Map<String, Object> map = new HashMap<>();
            map.put("orderCount", 7);
            map.put("rt", 15);
            map.put("issuccess", "false");
            map.put("tracer_id", "123qwe");
            goodsInfo.setExt(map);

            String indexName = "testgoods_" + DateTimeUtil.currentDate(DateTimeUtil.YYYYMMDD_PATTERN);
            if (!elasticsearchTemplate.indexExists(indexName)) {
                if (elasticsearchTemplate.createIndex(indexName)) {
                    ElasticsearchPersistentEntity persistentEntity = elasticsearchTemplate.getPersistentEntityFor(GoodsInfo.class);
                    XContentBuilder xContentBuilder = this.createXContentBuilder(persistentEntity, GoodsInfo.class);
                    elasticsearchTemplate.putMapping(indexName, "goods", xContentBuilder);
                }
            }

            IndexQueryBuilder indexQueryBuilder = new IndexQueryBuilder().withIndexName(indexName).withType("goods").withObject(goodsInfo);
            elasticsearchTemplate.index(indexQueryBuilder.build());
        }

        return "success";
    }


    private XContentBuilder createXContentBuilder(ElasticsearchPersistentEntity persistentEntity, Class clazz) {
        XContentBuilder xContentBuilder = null;
        try {
            ElasticsearchPersistentProperty property = (ElasticsearchPersistentProperty) persistentEntity.getRequiredIdProperty();
            xContentBuilder = MappingBuilder.buildMapping(clazz, persistentEntity.getIndexType(),
                    property.getFieldName(), null
            );
        } catch (Exception e) {
            throw new ElasticsearchException("Failed to build mapping for " + clazz.getSimpleName(), e);
        }
        return xContentBuilder;
    }


    @GetMapping("getSum")
    public Double getSum() {
        return this.sum();
    }


    @GetMapping("getCount")
    public Long getCount() {
        return this.count();
    }

    private long count() {
        ValueCountAggregationBuilder valueCountAggregationBuilder = AggregationBuilders.count("count").field("id");
        //AggregationBuilder aggregationBuilder = AggregationBuilders.nested("nested", "ext").subAggregation(sumBuilder);

        BoolQueryBuilder builder = QueryBuilders.boolQuery();
        builder.must(QueryBuilders.nestedQuery("ext", QueryBuilders.matchQuery("ext.issuccess", "false"), ScoreMode.None));

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withIndices("testgoods*")
                .withTypes("goods").withQuery(builder)
                .addAggregation((AbstractAggregationBuilder) valueCountAggregationBuilder).build();

        long saleAmount = elasticsearchTemplate.query(searchQuery, response -> {
            InternalValueCount sum = (InternalValueCount) response.getAggregations().asList().get(0);
            return sum.getValue();
        });
        return saleAmount;
    }


    private double sum() {
        AbstractAggregationBuilder aggregation = AggregationBuilders.terms("sum").field("ext.orderCount");

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withIndices("testgoods")
                .withTypes("goods")
                .addAggregation(AggregationBuilders.sum("sum").field("ext.orderCount"))
                .build();

        Aggregations aggregations = elasticsearchTemplate.query(searchQuery,
                new ResultsExtractor<Aggregations>() {
                    @Override
                    public Aggregations extract(SearchResponse response) {
                        return response.getAggregations();
                    }
                });
        return 0;
    }


    private SearchQuery getEntitySearchQuery(int pageNumber, int pageSize, String searchContent) {

        FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders.functionScoreQuery(QueryBuilders.matchPhraseQuery("name", searchContent));

        // 设置分页
        Pageable pageable = new PageRequest(pageNumber, pageSize);
        return new NativeSearchQueryBuilder()
                .withPageable(pageable)
                .withQuery(functionScoreQueryBuilder).build();
    }

}