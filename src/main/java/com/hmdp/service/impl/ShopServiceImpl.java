package com.hmdp.service.impl;

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
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public ShopServiceImpl(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 互斥锁的获取与释放方法
     * @param key
     * @return
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    @Resource
    private CacheClient cacheClient;

    /**
     * 根据id查询商铺信息（未解决缓存穿透and缓存击穿）
     * @param id
     * @return
     */
    @Override
    public Result queryById(Long id) {

        //1.从redis中查询商铺缓存
        String key = CACHE_SHOP_KEY + id;

        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2.判断该商铺是否存在
        if(StrUtil.isNotBlank(shopJson)){

            //3.存在，直接返回
            return Result.ok(JSONUtil.toBean(shopJson, Shop.class));
        }

        //4.未在redis中查到，去数据库中查
        Shop shop = getById(id);

        if(shop == null){

            //数据库没有，返回错误
            return Result.fail("店铺不存在");
        }

        //5.数据库有，写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),30L, TimeUnit.MINUTES);

        return Result.ok(shop);
    }

    /**
     * 根据id查询商铺信息（解决缓存穿透）
     * @param id
     * @return
     */
    public Result queryByIdWithPassThrough(Long id) {

        //1.从redis中查询商铺缓存
        String key = "cache:shop:" + id;

        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2.判断该商铺是否存在
        if(StrUtil.isNotBlank(shopJson)){

            //3.存在，直接返回
            return Result.ok(JSONUtil.toBean(shopJson, Shop.class));
        }

        //命中空字符串，返回
        if(shopJson != null){

            return Result.fail("店铺不存在");
        }

        //4.未在redis中查到，去数据库中查
        Shop shop = getById(id);

        if(shop == null){

            //数据库没有，redis写入null
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);

            return Result.fail("店铺不存在");
        }

        //5.数据库有，写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return Result.ok(shop);
    }

    /**
     * 根据id查询商铺信息（解决缓存穿透）
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id){
        String key = "cache:shop:" + id;

        //1.从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2.判断该商铺是否存在
        if(StrUtil.isNotBlank(shopJson)){

            //3.存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        //命中空字符串，返回
        if(shopJson != null){
            return null;
        }

        //4.不存在，根据id查询数据库
        Shop shop = getById(id);

        //4.1数据库中也不存在，将null写入redis缓存
        if(shop == null){
            stringRedisTemplate.opsForValue().set(key, "",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }

        //5.存在，写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return shop;
    }

    /**
     * 根据id查询商铺信息（用互斥锁解决缓存击穿）
     * @param id
     * @return
     */
    @Override
    public Shop queryWithMutex(Long id)  {

        String key = CACHE_SHOP_KEY + id;

        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //redis查到了，直接返回
        if(StrUtil.isNotBlank(shopJson)){
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        //redis没查到，判断是否是null
        if(shopJson != null){
            return null;
        }

        Shop shop = null;

        try {
            //获取互斥锁
            String lockKey = LOCK_SHOP_KEY + id;
            boolean isLock = tryLock(lockKey);

            if(!isLock){
                //获取锁失败，休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //获得锁成功
            //查询数据库
            shop = getById(id);

            //数据库中也不存在，将null写入redis缓存
            if(shop == null){
                stringRedisTemplate.opsForValue().set(key, "",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }

            //数据库中存在，将数据写入redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);


        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放锁
            unlock(LOCK_SHOP_KEY + id);
        }
        return shop;
    }


    /**
     * 根据id查询商铺信息
     * 使用逻辑过期解决缓存击穿问题
     * @param id
     * @return
     */
    public Shop queryWithLogicalExpire( Long id ) {

        String key = CACHE_SHOP_KEY + id;

        //1.从redis中查询商铺缓存,缓存格式是redisData的json形式
        String json = stringRedisTemplate.opsForValue().get(key);

        if(StrUtil.isBlank(json)){

            return null;
        }

        //2.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        //3.判断逻辑过期时间是否过期
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        if(expireTime.isAfter(LocalDateTime.now())){
            return shop;
        }

        //4.过期，需要缓存重建
        //4.1获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);

        if(isLock){

            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    this.saveShop2Redis(id,20L);
                }catch (Exception e){
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unlock(lockKey);
                }
            });

        }
        return shop;
    }

    @Override
    public Result update(Shop shop) {

        Long id = shop.getId();

        if (id == null) {
            return Result.fail("店铺id不能为空");
        }

        updateById(shop);

        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    public void saveShop2Redis(Long id,Long expireSeconds){

        //1.查询店铺数据
        Shop shop = getById(id);

        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        //3.写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }
}
