package com.example.demo;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
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

        Map<String,Integer> map=new HashMap<>();
        map.put("orderCount",77);
        map.put("rt",100);
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


    private double sum(){
        //QueryBuilder queryBuilder = QueryBuilders.nestedQuery("ext","ext.orderCoun",ScoreMode.None);
//        SumAggregationBuilder sumBuilder = AggregationBuilders.sum("sum").field("ext.orderCount");
        AvgAggregationBuilder sumBuilder = AggregationBuilders.avg("sum").field("ext.orderCount");
        AggregationBuilder aggregationBuilder = AggregationBuilders.nested("nested", "ext").subAggregation(sumBuilder);
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withIndices("testgoods")
                .withTypes("goods")
                .addAggregation((AbstractAggregationBuilder) aggregationBuilder).build();

        double saleAmount = elasticsearchTemplate.query(searchQuery, response -> {
            InternalNested sum = (InternalNested) response.getAggregations().asList().get(0);
//            return ((InternalSum)sum.getAggregations().get("sum")).getValue();
            return ((InternalAvg)sum.getAggregations().get("sum")).getValue();
        });
        return saleAmount;
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