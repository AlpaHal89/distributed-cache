import java.util.concurrent.CompletableFuture;

public interface DataSource<KEY, VALUE> {

    CompletableFuture<VALUE> load(KEY key);

    CompletableFuture<Void> persist(KEY key, VALUE value, long timestamp);
}
