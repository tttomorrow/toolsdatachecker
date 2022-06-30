package org.opengauss.datachecker.extract.cache;

import com.google.common.cache.*;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.springframework.lang.NonNull;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MetaDataCache {
    private static LoadingCache<String, TableMetadata> CACHE = null;

    /**
     * Initializing the Metadata Cache Method
     */
    public static void initCache() {
        CACHE =
                CacheBuilder.newBuilder()
                        //Set the concurrent read/write level based on the number of CPU cores;
                        .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                        // Size of the buffer pool
                        .maximumSize(Integer.MAX_VALUE)
                        // Removing a Listener
                        .removalListener(
                                (RemovalListener<String, TableMetadata>) remove -> log.info("cache: [{}], removed", remove))
                        .recordStats()
                        .build(
                                // Method of handing a Key that does not exist
                                new CacheLoader<>() {
                                    @Override
                                    public TableMetadata load(String tableName) {
                                        log.info("cache: [{}], does not exist", tableName);
                                        return null;
                                    }
                                });
        log.info("initialize table meta data cache");
    }

    /**
     * Save to the Cache k v
     *
     * @param key   Metadata key
     * @param value Metadata value of table
     */
    public static void put(@NonNull String key, TableMetadata value) {
        try {
            log.info("put in cache:[{}]-[{}]", key, value);
            CACHE.put(key, value);
        } catch (Exception exception) {
            log.error("put in cache exception ", exception);
        }
    }

    /**
     * Batch storage  to the cache
     *
     * @param map map of key,value ,that have some table metadata
     */
    public static void putMap(@NonNull Map<String, TableMetadata> map) {
        try {
            CACHE.putAll(map);
            map.forEach((key, value) -> log.debug("batch cache deposit:[{},{}]", key, value));
        } catch (Exception exception) {
            log.error("batch storage cache exception", exception);
        }
    }

    /**
     * get cache
     *
     * @param key table name as cached key
     */
    public static TableMetadata get(String key) {
        try {
            return CACHE.get(key);
        } catch (Exception exception) {
            log.error("get cache exception", exception);
            return null;
        }
    }

    /**
     * Obtains all cached key sets
     *
     * @return keys cached key sets
     */
    public static Set<String> getAllKeys() {
        try {
            return CACHE.asMap().keySet();
        } catch (Exception exception) {
            log.error("get cache exception", exception);
            return null;
        }
    }

    /**
     * Clear all cache information
     */
    public static void removeAll() {
        if (Objects.nonNull(CACHE) && CACHE.size() > 0) {
            log.info("clear cache information");
            CACHE.cleanUp();
        }
    }

    /**
     * Specify cache information clearly based on key values
     *
     * @param key key values of the cache to be cleared
     */
    public static void remove(String key) {
        if (Objects.nonNull(CACHE) && CACHE.size() > 0) {
            log.info("clear cache information");
            CACHE.asMap().remove(key);
        }
    }
}
