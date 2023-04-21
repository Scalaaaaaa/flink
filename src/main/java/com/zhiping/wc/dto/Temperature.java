package com.zhiping.wc.dto;

import lombok.Data;
import lombok.ToString;

import java.math.BigDecimal;

@Data
@ToString
public class Temperature {
    private Integer deviceId;
    private Integer deviceType;
    private Double temperature;
    private Long ts;

    @Override
    public String toString() {
        return "Temperature{" +
                "deviceId=" + deviceId +
                ", deviceType=" + deviceType +
                ", temperature=" + temperature +
                '}';
    }

}
