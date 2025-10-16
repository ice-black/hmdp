package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
        //Shop shop = queryWithPassThrough(id);
        //Shop shop = cacheClient
        //        .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, 10L, TimeUnit.SECONDS);

        // 互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存击穿
        //Shop shop = queryWithLogicExpire(id);
        Shop shop = cacheClient
                .queryWithLogicExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, LOCK_SHOP_KEY, CACHE_NULL_TTL, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        // 7.返回
        return Result.ok(shop);
    }

    //private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    //
    //private Shop queryWithLogicExpire(Long id){
    //    String key = RedisConstants.CACHE_SHOP_KEY + id;
    //    // 1.从redis查询商铺缓存
    //    String shopJson = stringRedisTemplate.opsForValue().get(key);
    //    // 2.判断是否存在
    //    if (StrUtil.isBlank(shopJson)) {
    //        // 3.不存在，直接返回
    //        return null;
    //    }
    //    //4.命中,需要先把json反序列化为对象
    //    RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
    //    JSONObject data = (JSONObject) redisData.getData();
    //    Shop shop = JSONUtil.toBean(data, Shop.class);
    //    LocalDateTime expireTime = redisData.getExpireTime();
    //    //5.判断是否过期
    //    if (expireTime.isAfter(LocalDateTime.now())) {
    //        //5.1.未过期,直接返回店铺信息
    //        return shop;
    //    }
    //    //5.2.已过期,需要缓存重建
    //    //6.缓存重建
    //    //6.1.获取互斥锁
    //    String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
    //    boolean isLock = tryLock(lockKey);
    //    //6.2.判断是否获取锁成功
    //    if (!isLock) {
    //        //6.3.成功,开启独立线程,实现缓存重建
    //        CACHE_REBUILD_EXECUTOR.submit(() -> {
    //            try {
    //                //6.5.重建缓存
    //                saveShop2Redis(id, 20L);
    //            } catch (Exception e) {
    //                throw new RuntimeException(e);
    //            } finally {
    //                unLock(lockKey);
    //            }
    //        });
    //    }
    //    //6.4.返回过期的商铺信息
    //    return shop;
    //}

    private Shop queryWithMutex(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 判断命中的是否是空值
        if(shopJson != null){
            return null;
        }
        //4.实现缓存重建
        //4.1.获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2.判断是否获取成功
            if (!isLock) {
                //4.3.失败,则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //4.4.成功,根据id查询数据库
            // 4.不存在，根据id查询数据库
            shop = this.getById(id);
            if (shop == null) {
                // 5.不存在，将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 返回错误信息
                return null;
            }
            // 6.存在，写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 7.释放互斥锁
            unLock(lockKey);
        }
        // 8.返回
        return shop;
    }

    //private Shop queryWithPassThrough(Long id){
    //    String key = RedisConstants.CACHE_SHOP_KEY + id;
    //    // 1.从redis查询商铺缓存
    //    String shopJson = stringRedisTemplate.opsForValue().get(key);
    //    // 2.判断是否存在
    //    if (StrUtil.isNotBlank(shopJson)) {
    //        // 3.存在，直接返回
    //        return JSONUtil.toBean(shopJson, Shop.class);
    //    }
    //    // 判断命中的是否是空值
    //    if(shopJson != null){
    //        return null;
    //    }
    //    // 4.不存在，根据id查询数据库
    //    Shop shop = this.getById(id);
    //    if (shop == null) {
    //        // 5.不存在，将空值写入redis
    //        stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
    //        // 返回错误信息
    //        return null;
    //    }
    //    // 6.存在，写入redis
    //    stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
    //    return shop;
    //}

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    //private void saveShop2Redis(Long id, Long expireSeconds){
    //    // 1.查询商铺数据
    //    Shop shop = getById(id);
    //    // 2.封装逻辑过期时间
    //    RedisData redisData = new RedisData();
    //    redisData.setData(shop);
    //    redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
    //    // 3.写入redis
    //    stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    //}

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
