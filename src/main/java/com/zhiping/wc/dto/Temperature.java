package com.zhiping.wc.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class Temperature {
    private Long deviceId;
    private Integer deviceType;
    private BigDecimal temperature;
}
