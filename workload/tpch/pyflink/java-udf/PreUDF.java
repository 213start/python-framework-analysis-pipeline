package com.huawei;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @since 2025/12/23
 */
public class PreUDF extends ScalarFunction {

    @Override
    public void open(FunctionContext context) throws Exception {
        System.out.printf("[Benchmark Start]: Start Time(ns)=%d%n", System.nanoTime());
    }

    @Override
    public void close() throws Exception {
        System.out.printf("[PreUDF End]: End Time(ns)=%d%n", System.nanoTime());
    }

    @DataTypeHint("ROW<price DOUBLE, javaStartTime BIGINT>")
    public Row eval(Double price) {
        return Row.of(price, System.nanoTime());
    }
}

