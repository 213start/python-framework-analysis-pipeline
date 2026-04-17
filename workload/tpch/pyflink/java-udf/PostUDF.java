package com.huawei;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.concurrent.atomic.AtomicLong;

/**
 * PostUDF — records java_end_time, computes framework overhead per record,
 * accumulates batch statistics, and outputs structured summary on close().
 *
 * Returns a single BIGINT (overhead) per record to avoid Flink's
 * multi-evaluation of ROW-returning scalar functions.
 *
 * Batch summary (printed on close()):
 *   - recordCount
 *   - avgFrameworkOverheadNs  (per record)
 *   - avgPyDurationNs         (per record)
 *   - totalFrameworkOverheadNs
 *   - totalPyDurationNs
 */
public class PostUDF extends ScalarFunction {

    private AtomicLong count;
    private AtomicLong sumOverhead;
    private AtomicLong sumPyDuration;

    @Override
    public void open(FunctionContext context) throws Exception {
        System.out.printf("[PostUDF Start]: Start Time(ns)=%d%n", System.nanoTime());
        count = new AtomicLong();
        sumOverhead = new AtomicLong();
        sumPyDuration = new AtomicLong();
    }

    @Override
    public void close() throws Exception {
        long closeTimeNs = System.nanoTime();
        long n = count.get();
        if (n > 0) {
            double avgOverhead = (double) sumOverhead.get() / n;
            double avgPyDuration = (double) sumPyDuration.get() / n;
            System.out.printf("[PostUDF End]: End Time(ns)=%d%n", closeTimeNs);
            System.out.printf("[BENCHMARK_SUMMARY] {\"recordCount\": %d, \"avgFrameworkOverheadNs\": %.1f, \"avgPyDurationNs\": %.1f, \"totalFrameworkOverheadNs\": %d, \"totalPyDurationNs\": %d}%n",
                n, avgOverhead, avgPyDuration, sumOverhead.get(), sumPyDuration.get());
        } else {
            System.out.printf("[PostUDF End]: End Time(ns)=%d, no records processed%n", closeTimeNs);
        }
    }

    @DataTypeHint("BIGINT")
    public Long eval(Long startTimeNs, Long pyDurationNs) {
        long endTimeNs = System.nanoTime();
        long totalRoundTrip = endTimeNs - startTimeNs;
        long overhead = totalRoundTrip - pyDurationNs;

        count.getAndAdd(1);
        sumOverhead.getAndAdd(overhead);
        if (pyDurationNs != null) {
            sumPyDuration.getAndAdd(pyDurationNs);
        }

        // Periodic logging every 1M records
        if (count.get() % 1000000 == 0) {
            double avg = (double) sumOverhead.get() / count.get();
            System.out.printf(
                "[Benchmark Counter]: Count=%d, Avg Overhead=%f ns, Last Overhead=%d ns%n",
                count.get(), avg, overhead);
        }

        return overhead;
    }
}
