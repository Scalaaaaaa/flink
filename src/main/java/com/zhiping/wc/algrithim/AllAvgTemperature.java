package com.zhiping.wc.algrithim;

import com.zhiping.wc.dto.JustAvgTemperature;
import com.zhiping.wc.dto.Temperature;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class AllAvgTemperature implements AggregateFunction<Temperature, Tuple2<BigDecimal, Integer>, JustAvgTemperature> {
    @Override
    public Tuple2<BigDecimal, Integer> createAccumulator() {
        return Tuple2.of(BigDecimal.ZERO, 0);
    }

    @Override
    public Tuple2<BigDecimal, Integer> add(Temperature temperature, Tuple2<BigDecimal, Integer> acc) {
        return Tuple2.of(acc.f0.add(temperature.getTemperature()), acc.f1 + 1);
    }

    @Override
    public JustAvgTemperature getResult(Tuple2<BigDecimal, Integer> acc) {
        return new JustAvgTemperature(acc.f0.divide(new BigDecimal(acc.f1), 2, RoundingMode.HALF_UP));
    }

    @Override
    public Tuple2<BigDecimal, Integer> merge(Tuple2<BigDecimal, Integer> acc1, Tuple2<BigDecimal, Integer> acc2) {
        return Tuple2.of(acc1.f0.add(acc2.f0), acc1.f1 + acc2.f1);
    }
}
