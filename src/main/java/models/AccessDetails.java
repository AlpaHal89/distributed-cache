package models;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public class AccessDetails {
    // LongAdder is usually preferable to AtomicLong when multiple threads update a common sum that is used for purposes such as collecting statistics,
    // not for fine-grained synchronization control. Under low update contention, the two classes have similar characteristics.
    // But under high contention, expected throughput of this class is significantly higher, at the expense of higher space consumption.
    //Instead of using just one value to maintain the current state, this class uses an array of states to distribute the contention to different memory locations
    //To prevent false sharing. the Striped64 implementation adds enough padding around each state to make sure that each state resides in its own cache line using @Contended annotation
    //The result of the counter in the LongAdder is not available until we call the sum() method.
    //That method will iterate over all values of the underneath array, and sum those values returning the proper value.
    //We need to be careful though because the call to the sum() method can be very costly
    private final LongAdder accessCount;
    private long lastAccessTime;

    public AccessDetails(long lastAccessTime) {
        accessCount = new LongAdder();
        this.lastAccessTime = lastAccessTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public int getAccessCount() {
        return (int) accessCount.sum();
    }

    public AccessDetails update(long lastAccessTime) {
        final AccessDetails accessDetails = new AccessDetails(lastAccessTime); //both time and count updated
        accessDetails.accessCount.increment();
        return accessDetails;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccessDetails that = (AccessDetails) o;
        return lastAccessTime == that.lastAccessTime &&
                this.getAccessCount() == that.getAccessCount();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getAccessCount(), lastAccessTime);
    }

    @Override
    public String toString() {
        return "AccessDetails{" +
                "accessCount=" + accessCount +
                ", lastAccessTime=" + lastAccessTime +
                '}';
    }
}
