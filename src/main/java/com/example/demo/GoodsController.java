package com.example.demo;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.InternalValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/")
public class GoodsController {

    @Autowired
    private GoodsRepository goodsRepository;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    //http://localhost:8081/save
    @RequestMapping("save")
    public String save() {
        GoodsInfo goodsInfo = new GoodsInfo(System.currentTimeMillis(),
                "商品" + System.currentTimeMillis(), "这是一个测试商品");

        Map<String, Object> map = new HashMap<>();
        map.put("orderCount", 7);
        map.put("rt", 15);
        map.put("issuccess", "false");
        map.put("tracer_id", "123qwe");
        goodsInfo.setExt(map);
        goodsRepository.save(goodsInfo);
        return "success";
    }

    //http://localhost:8081/delete?id=1525415333329
    @GetMapping("delete")
    public String delete(long id) {
        goodsRepository.deleteById(id);
        return "success";
    }

    //http://localhost:8081/update?id=1525417362754&name=修改&description=修改
    @GetMapping("update")
    public String update(long id, String name, String description) {
        GoodsInfo goodsInfo = new GoodsInfo(id,
                name, description);
        goodsRepository.save(goodsInfo);
        return "success";
    }

    //http://localhost:8081/getOne?id=1525417362754
    @GetMapping("getOne")
    public GoodsInfo getOne(long id) {
        Optional<GoodsInfo> optionalGoodsInfo = goodsRepository.findById(id);
        return optionalGoodsInfo.get();
    }


    //每页数量
    private Integer PAGESIZE = 10;

    //http://localhost:8081/getGoodsList?query=商品
    //http://localhost:8081/getGoodsList?query=商品&pageNumber=1
    //根据关键字"商品"去查询列表，name或者description包含的都查询
    @GetMapping("getGoodsList")
    public List<GoodsInfo> getList(Integer pageNumber, String query) {
        if (pageNumber == null) pageNumber = 0;
        //es搜索默认第一页页码是0
        SearchQuery searchQuery = getEntitySearchQuery(pageNumber, PAGESIZE, query);
        Page<GoodsInfo> goodsPage = goodsRepository.search(searchQuery);
        return goodsPage.getContent();
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
                .withIndices("testgoods")
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