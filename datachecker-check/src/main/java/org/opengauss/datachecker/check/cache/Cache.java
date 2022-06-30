package org.opengauss.datachecker.check.cache;

import java.util.Set;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
public interface Cache<K, V> {

    /**
     * 初始化缓存 并给键值设置默认值
     *
     * @param keys 缓存键值
     */
    void init(Set<K> keys);

    /**
     * 添加键值对到缓存
     *
     * @param key   键
     * @param value 值
     */
    void put(K key, V value);

    /**
     * 根据key查询缓存
     *
     * @param key 缓存key
     * @return 缓存value
     */
    V get(K key);

    /**
     * 获取缓存Key集合
     *
     * @return Key集合
     */
    Set<K> getKeys();

    /**
     * 更新缓存数据
     *
     * @param key   缓存key
     * @param value 缓存value
     * @return 更新后的缓存value
     */
    V update(K key, V value);

    /**
     * 删除指定key缓存
     *
     * @param key key
     */
    void remove(K key);

    /**
     * 清除全部缓存
     */
    void removeAll();

    /**
     * 缓存持久化接口 将缓存信息持久化到本地
     */
    void persistent();
}
