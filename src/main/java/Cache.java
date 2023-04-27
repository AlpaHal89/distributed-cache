import events.*;
import models.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;

public class Cache<KEY, VALUE> {
    private final int maximumSize;
    private final FetchAlgorithm fetchAlgorithm;
    private final Duration expiryTime; //expiry mentioned in Duration secs
    private final Map<KEY, CompletableFuture<Record<KEY, VALUE>>> cache; //simple concurrenthashmap which holds future object against key as could be loaded from DB
    private final ConcurrentSkipListMap<AccessDetails, List<KEY>> priorityQueue; //access time/freq sorted rIdx holding list of keys
    private final ConcurrentSkipListMap<Long, List<KEY>> expiryQueue; //expiry time sorted rIdx holding list of keys
    private final DataSource<KEY, VALUE> dataSource;
    private final ExecutorService[] executorPool; //size of this is number of concurrent requests we will take
    private final Timer timer;

    protected Cache(final int maximumSize,
                    final Duration expiryTime,
                    final FetchAlgorithm fetchAlgorithm,
                    final EvictionAlgorithm evictionAlgorithm,
                    final DataSource<KEY, VALUE> dataSource,
                    final Set<KEY> keysToEagerlyLoad,
                    final Timer timer,
                    final int poolSize) {
        this.expiryTime = expiryTime;
        this.maximumSize = maximumSize;
        this.fetchAlgorithm = fetchAlgorithm;
        this.timer = timer;
        this.cache = new ConcurrentHashMap<>();
        //this.eventQueue = new CopyOnWriteArrayList<>();

        this.executorPool = new ExecutorService[poolSize];
        for (int i = 0; i < poolSize; i++) {
            executorPool[i] = Executors.newSingleThreadExecutor();
        }

        //depending on algo sorting of same PQ changes
        priorityQueue = new ConcurrentSkipListMap<>((first, second) -> {
            final var accessTimeDifference = (int) (first.getLastAccessTime() - second.getLastAccessTime());
            if (evictionAlgorithm.equals(EvictionAlgorithm.LRU)) {
                return accessTimeDifference;
            } else {
                final var accessCountDifference = first.getAccessCount() - second.getAccessCount();
                return accessCountDifference != 0 ? accessCountDifference : accessTimeDifference;
            }
        });
        //cache has expiry as well
        expiryQueue = new ConcurrentSkipListMap<>();

        //DB behind the cache
        this.dataSource = dataSource;
        final var eagerLoading = keysToEagerlyLoad.stream()
                .map(key -> getThreadFor(key, addToCache(key, loadFromDB(dataSource, key))))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(eagerLoading).join();
    }

    //
    private <U> CompletableFuture<U> getThreadFor(KEY key, CompletableFuture<U> task) {
        //thenCompose flatmaps the CF
        return CompletableFuture.supplyAsync(() -> task, executorPool[Math.abs(key.hashCode() % executorPool.length)]).thenCompose(Function.identity());
    }

    //get and set need to be in operated in sequence so we use a single thread executor for the key
    public CompletableFuture<VALUE> get(KEY key) {
        return getThreadFor(key, getFromCache(key));
    }

    public CompletableFuture<Void> set(KEY key, VALUE value) {
        return getThreadFor(key, setInCache(key, value));
    }

    private CompletableFuture<VALUE> getFromCache(KEY key) {
        final CompletableFuture<Record<KEY, VALUE>> result;
        if (!cache.containsKey(key)) {
            result = addToCache(key, loadFromDB(dataSource, key));
        } else {
            //return key and use thencompose cause next method(for expiry check/complete the future) also returns a CF so we need to flatmap the result
            result = cache.get(key).thenCompose(record -> {
                if (hasExpired(record)) {
                    priorityQueue.get(record.getAccessDetails()).remove(key);
                    expiryQueue.get(record.getInsertionTime()).remove(key);
                    return addToCache(key, loadFromDB(dataSource, key)); //expired so reload updated value from DB!
                } else {
                    return CompletableFuture.completedFuture(record); //will give data on CF get()
                }
            });
        }
        //CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> 1).thenApply(x -> x+1);
        //CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> 1).thenCompose(x -> CompletableFuture.supplyAsync(() -> x+1));
        return result.thenApply(record -> {
            priorityQueue.get(record.getAccessDetails()).remove(key);
            final AccessDetails updatedAccessDetails = record.getAccessDetails().update(timer.getCurrentTime());
            priorityQueue.computeIfAbsent(updatedAccessDetails, k -> new CopyOnWriteArrayList<>()).add(key);
            record.setAccessDetails(updatedAccessDetails);
            return record.getValue(); //on every get update the access details in the record and the rIdx
        });
    }

    //CompletableFuture unchecked exception handling
    //exceptionally() only input is exception. need to return a value for CF. caller will get error object. catch and recover.
    //or rethrow new exception
    //whenComplete() both success and exception is input but no return. caller will get CompletionException. catch and no recovery
    //for checked exceptions functional programming says wrap in try-catch and throw unchecked exception
    public CompletableFuture<Void> setInCache(KEY key, VALUE value) {
        CompletableFuture<Void> result = CompletableFuture.completedFuture(null); //initialzed CF but still not given to client
        if (cache.containsKey(key)) {
            //remove old entry to update access
            result = cache.remove(key)
                    .thenAccept(oldRecord -> {
                        priorityQueue.get(oldRecord.getAccessDetails()).remove(key);
                        expiryQueue.get(oldRecord.getInsertionTime()).remove(key);
                    });
        }
        //if not in cache then chain add to cache then write to db
        return result.thenCompose(__ -> {
            try {
                return addToCache(key, CompletableFuture.completedFuture(value));
            } catch (ExecutionException |InterruptedException e) {
                throw new CompletionException(e.getCause());  //wrap and rethrow unchecked exception
            }
        }).thenCompose(record -> {
            final CompletableFuture<Void> writeOperation = persistRecord(record);
            return fetchAlgorithm == FetchAlgorithm.WRITE_THROUGH ? writeOperation : CompletableFuture.completedFuture(null);
        });
    }

    //Future loaded from DB if cache miss/static data in completedFuture()
    private CompletableFuture<Record<KEY, VALUE>> addToCache(final KEY key, final CompletableFuture<VALUE> valueFuture) throws ExecutionException, InterruptedException {
        checkCapacity();
        final var recordFuture = valueFuture.thenApply(value -> {
            final Record<KEY, VALUE> record = new Record<>(key, value, timer.getCurrentTime());
            expiryQueue.computeIfAbsent(record.getInsertionTime(), k -> new CopyOnWriteArrayList<>()).add(key);
            priorityQueue.computeIfAbsent(record.getAccessDetails(), k -> new CopyOnWriteArrayList<>()).add(key);
            return record;
        });
        cache.put(key, recordFuture); //add to cache and both rIdx
        return recordFuture;
    }

    //need to be synchronized!!
    private synchronized void checkCapacity() throws ExecutionException, InterruptedException, TimeoutException {
        if (cache.size() >= maximumSize) {
            //check if top entry expired in loop and remove keys from cache and rIdx
            while (!expiryQueue.isEmpty() && hasExpired(expiryQueue.firstKey())) {
                final List<KEY> keys = expiryQueue.pollFirstEntry().getValue();
                for (final KEY key : keys) {
                    //vimp. always get and add with timeout when call might get blocked.
                    // join() doesnt throw checked exception but has no timeout
                    final Record<KEY, VALUE> expiredRecord = cache.remove(key).get(100, TimeUnit.SECONDS);
                    priorityQueue.remove(expiredRecord.getAccessDetails());
                }
            }
            //check if rIdx values became empty
            List<KEY> keys = priorityQueue.firstEntry().getValue();
            while (keys.isEmpty()) {
                priorityQueue.pollFirstEntry();
            }
        }
    }

    private CompletableFuture<Void> persistRecord(final Record<KEY, VALUE> record) {
        return dataSource.persist(record.getKey(), record.getValue(), record.getInsertionTime());
    }

    private boolean hasExpired(final Record<KEY, VALUE> record) {
        return hasExpired(record.getInsertionTime());
    }

    private boolean hasExpired(final Long time) {
        return Duration.ofNanos(timer.getCurrentTime() - time).compareTo(expiryTime) > 0; //duration comparison vimp
    }

    private CompletableFuture<VALUE> loadFromDB(final DataSource<KEY, VALUE> dataSource, KEY key) {
        return dataSource.load(key);
    }
}

