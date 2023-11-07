/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.benmanes.caffeine.cache;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This class provides a skeletal implementation of the {@link LoadingCache} interface to minimize
 * the effort required to implement a {@link LocalCache}.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
interface LocalLoadingCache<K, V> extends LocalManualCache<K, V>, LoadingCache<K, V> {
  Logger logger = Logger.getLogger(LocalLoadingCache.class.getName());

  /** Returns the {@link CacheLoader} used by this cache. */
  CacheLoader<? super K, V> cacheLoader();

  /** Returns the {@link CacheLoader#load} as a mapping function. */
  Function<K, V> mappingFunction();

  /** Returns the {@link CacheLoader#loadAll} as a mapping function, if implemented. */
  @Nullable Function<Iterable<? extends K>, Map<K, V>> bulkMappingFunction();

  @Override
  default @Nullable V get(K key) {
    return cache().computeIfAbsent(key, mappingFunction());
  }

  @Override
  default Map<K, V> getAll(Iterable<? extends K> keys) {
    Function<Iterable<? extends K>, Map<K, V>> mappingFunction = bulkMappingFunction();
    return (mappingFunction == null)
        ? loadSequentially(keys)
        : getAll(keys, mappingFunction);
  }

  /** Sequentially loads each missing entry. */
  default Map<K, V> loadSequentially(Iterable<? extends K> keys) {
    Set<K> uniqueKeys = new LinkedHashSet<>();
    for (K key : keys) {
      uniqueKeys.add(key);
    }

    int count = 0;
    Map<K, V> result = new LinkedHashMap<>(uniqueKeys.size());
    try {
      for (K key : uniqueKeys) {
        count++;

        V value = get(key);
        if (value != null) {
          result.put(key, value);
        }
      }
    } catch (Throwable t) {
      cache().statsCounter().recordMisses(uniqueKeys.size() - count);
      throw t;
    }
    return Collections.unmodifiableMap(result);
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  default void refresh(K key) {

    // key不能为空
    requireNonNull(key);

    // 新建数组存储旧值的写入时间
    long[] writeTime = new long[1];

    // 获取当前时间
    long startTime = cache().statsTicker().read();

    // 获取旧值
    V oldValue = cache().getIfPresentQuietly(key, writeTime);

    // 如果旧值为空，则异步加载，如果不为空，则异步重载，调用不同方法
    // 生成一个future
    CompletableFuture<V> refreshFuture = (oldValue == null)
        ? cacheLoader().asyncLoad(key, cache().executor())
        : cacheLoader().asyncReload(key, oldValue, cache().executor());

    // 新值加载完成（即刷新完成）的后置处理，包含记录加载时间，替换旧值
    refreshFuture.whenComplete((newValue, error) -> {

      // 计算加载时间
      long loadTime = cache().statsTicker().read() - startTime;

      // 如果加载异常，打印日志，并记录加载失败的耗时
      if (error != null) {
        logger.log(Level.WARNING, "Exception thrown during refresh", error);
        cache().statsCounter().recordLoadFailure(loadTime);
        return;
      }

      // 新建布尔数据，记录旧值是否被替换
      boolean[] discard = new boolean[1];
      cache().compute(key, (k, currentValue) -> {

        // 当前旧值为空，直接返回新值
        if (currentValue == null) {
          return newValue;
        }

        // 当前旧值等于加载新值前获取到的旧值
        else if (currentValue == oldValue) {

          // 获取加载新值前获取到的旧值的写入时间
          long expectedWriteTime = writeTime[0];

          // 如果当前缓存会在写入后过期/刷新，则获取key当前对应值的写入时间
          if (cache().hasWriteTime()) {
            cache().getIfPresentQuietly(key, writeTime);
          }

          // 如果当前旧值等于加载新值前获取到的旧值，且两者的写入时间一致，说明是同一个旧值
          // 直接返回新值即可
          if (writeTime[0] == expectedWriteTime) {
            return newValue;
          }
        }

        // 走到这里，说明在加载新值过程中，缓存里key对应的值已经被刷新过，丢弃本次加载到的新值，返回currentValue
        discard[0] = true;
        return currentValue;
      }, /* recordMiss */ false, /* recordLoad */ false, /* recordLoadFailure */ true);

      if (discard[0] && cache().hasRemovalListener()) {
        cache().notifyRemoval(key, newValue, RemovalCause.REPLACED);
      }

      // 进行指标记录
      // 如果新值为空，则默认为加载失败（无论加载成功或失败）
      if (newValue == null) {
        cache().statsCounter().recordLoadFailure(loadTime);
      } else {
        cache().statsCounter().recordLoadSuccess(loadTime);
      }
    });
  }

  /** Returns a mapping function that adapts to {@link CacheLoader#load}. */
  static <K, V> Function<K, V> newMappingFunction(CacheLoader<? super K, V> cacheLoader) {
    return key -> {
      try {
        return cacheLoader.load(key);
      } catch (RuntimeException e) {
        throw e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CompletionException(e);
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    };
  }

  /** Returns a mapping function that adapts to {@link CacheLoader#loadAll}, if implemented. */
  static <K, V> @Nullable Function<Iterable<? extends K>, Map<K, V>> newBulkMappingFunction(
      CacheLoader<? super K, V> cacheLoader) {
    if (!hasLoadAll(cacheLoader)) {
      return null;
    }
    return keysToLoad -> {
      try {
        @SuppressWarnings("unchecked")
        Map<K, V> loaded = (Map<K, V>) cacheLoader.loadAll(keysToLoad);
        return loaded;
      } catch (RuntimeException e) {
        throw e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CompletionException(e);
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    };
  }

  /** Returns whether the supplied cache loader has bulk load functionality. */
  static boolean hasLoadAll(CacheLoader<?, ?> loader) {
    try {
      Method classLoadAll = loader.getClass().getMethod("loadAll", Iterable.class);
      Method defaultLoadAll = CacheLoader.class.getMethod("loadAll", Iterable.class);
      return !classLoadAll.equals(defaultLoadAll);
    } catch (NoSuchMethodException | SecurityException e) {
      logger.log(Level.WARNING, "Cannot determine if CacheLoader can bulk load", e);
      return false;
    }
  }
}
