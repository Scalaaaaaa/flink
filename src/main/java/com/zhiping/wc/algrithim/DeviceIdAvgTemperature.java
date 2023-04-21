package com.zhiping.wc.algrithim;

import com.zhiping.wc.dto.DeviceAvgTemperature;
import com.zhiping.wc.dto.Temperature;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class DeviceIdAvgTemperature  implements AggregateFunction<Temperature,
        Tuple3<Long ,BigDecimal, Integer>, DeviceAvgTemperature> {
    @Override
    public Tuple3<Long, BigDecimal, Integer> createAccumulator() {
        return Tuple3.of(0L,BigDecimal.ZERO,0);
    }

    @Override
    public Tuple3<Long, BigDecimal, Integer> add(Temperature temperature, Tuple3<Long, BigDecimal, Integer> acc) {
        acc.f0 = temperature.getDeviceId().longValue();
        acc.f1 = acc.f1.add(new BigDecimal(temperature.getTemperature()));
        acc.f2 = acc.f2 + 1;
        return acc;
    }

    @Override
    public DeviceAvgTemperature getResult(Tuple3<Long, BigDecimal, Integer> acc) {
        BigDecimal temRes = acc.f1.divide(new BigDecimal(acc.f2), 2, RoundingMode.HALF_UP);
        return new DeviceAvgTemperature(acc.f0, temRes,"user","{\"deviceId\":"+acc.f0+",\"avgTemperature\":"+temRes+"}");
    }

    @Override
    public Tuple3<Long, BigDecimal, Integer> merge(Tuple3<Long, BigDecimal, Integer> longBigDecimalIntegerTuple3, Tuple3<Long, BigDecimal, Integer> acc1) {
        return null;
    }
}
