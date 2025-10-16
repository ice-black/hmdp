package com.hmdp.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // string实现
    //@Override
    //public Result queryTypeList() {
    //    String shopTypeJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE_KEY);
    //    if (StrUtil.isNotBlank(shopTypeJson)) {
    //        return Result.ok(JSONUtil.toList(shopTypeJson, ShopType.class));
    //    }
    //    List<ShopType> shopTypes = this.list();
    //    if (shopTypes == null) {
    //        return Result.fail("店铺类型不存在！");
    //    }
    //    stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shopTypes));
    //    return Result.ok(shopTypes);
    //}

    // list实现
    @Override
    public Result queryTypeList() {
        // opsForList写法
        // public static final String CACHE_SHOP_TYPE_KEY = "cache:shopType:";
        // 1. 从Redis查询 商铺类型缓存 , end:-1 表示取全部数据
        List<String> shopTypeJsonList = stringRedisTemplate.opsForList().range(CACHE_SHOP_TYPE_KEY, 0, -1);
        // 2. 有就直接返回
        if (CollectionUtil.isNotEmpty(shopTypeJsonList)) {
            // JSON字符串转对象 排序后返回
            List<ShopType> shopTypeList = JSONUtil.toList(shopTypeJsonList.toString(), ShopType.class);
            Collections.sort(shopTypeList, (o1, o2) -> o1.getSort() - o2.getSort());
            return Result.ok(shopTypeList);
        }
        // 3. 没有就向数据库查询 MP的query()拿来用
        List<ShopType> shopTypes = query().orderByAsc("sort").list();

        // 4. 不存在，返回错误
        if (CollectionUtil.isEmpty(shopTypes)){
            return Result.fail("商铺类型不存在...");
        }
        // 5. 存在， 写入Redis，这里使用Redis的List类型，String类型，就是直接所有都写在一起，对内存开销比较大。
        // 要将List中的每个元素(元素类型ShopType) ，每个元素都要单独转成JSON，使用stream流的map映射
        // Hutools里的 BeanUtil.copyToList 本来想模仿UserService中的写法，
        // 传入一个CopyOptions的，但是setFieldValueEditor貌似只对beanToMap有效
        // 改用流的形式转换每个list元素
        List<String> shopTypesJson = shopTypes.stream()
                .map(shopType -> JSONUtil.toJsonStr(shopType))
                .collect(Collectors.toList());
        // 因为从数据库读出来的时候已经是按照顺序读出来的，这里想要维持顺序必须从右边push，类似队列
        stringRedisTemplate.opsForList().rightPushAll(CACHE_SHOP_TYPE_KEY, shopTypesJson);
        // 5. 返回
        return Result.ok(shopTypes);
    }


    // zset实现
    //@Override
    //public Result queryTypeList() {
    //    // 1. 从Redis的ZSet中查询商铺类型缓存（按分数升序获取，与sort字段对应）
    //    Set<String> shopTypeJsonSet = stringRedisTemplate.opsForZSet()
    //            .range(CACHE_SHOP_TYPE_KEY, 0, -1);
    //    // 2. 缓存存在，直接转换并返回
    //    if (CollectionUtil.isNotEmpty(shopTypeJsonSet)) {
    //        List<ShopType> shopTypeList = shopTypeJsonSet.stream().map(json -> JSONUtil.toBean(json, ShopType.class)).collect(Collectors.toList());
    //        return Result.ok(shopTypeList);
    //    }
    //    // 3. 缓存不存在，从数据库查询（按sort字段升序）
    //    List<ShopType> shopTypes = query().orderByAsc("sort").list();
    //
    //    // 4. 数据库中不存在，返回错误
    //    if (CollectionUtil.isEmpty(shopTypes)) {
    //        return Result.fail("商铺类型不存在...");
    //    }
    //    // 5. 将查询结果批量写入Redis的ZSet（以sort字段为分数，实现天然排序）
    //    // 构建ZSet的键值对集合（value为JSON字符串，score为sort值）
    //    //Set<ZSetOperations.TypedTuple<String>> typedTuples = shopTypes.stream()
    //    //        .map(shopType -> {
    //    //            // 每个元素封装为TypedTuple，包含JSON字符串和对应的score（sort值）
    //    //            return new DefaultTypedTuple<>(
    //    //                    JSONUtil.toJsonStr(shopType),
    //    //                    (double) shopType.getSort()
    //    //            );
    //    //        })
    //    //        .collect(Collectors.toSet());
    //    //
    //    //// 批量插入ZSet，一次网络请求完成所有元素的存储
    //    //stringRedisTemplate.opsForZSet().add(CACHE_SHOP_TYPE_KEY, typedTuples);
    //
    //    // 5. 将查询结果写入Redis的ZSet（以sort字段为分数，实现天然排序）
    //    // 构建ZSet的键值对集合（value为JSON字符串，score为sort值）
    //    shopTypes.forEach(shopType -> {
    //        stringRedisTemplate.opsForZSet().add(
    //                CACHE_SHOP_TYPE_KEY,
    //                JSONUtil.toJsonStr(shopType),
    //                shopType.getSort() // 以sort作为分数，保证有序性
    //        );
    //    });
    //    return Result.ok(shopTypes);
    //}
}
