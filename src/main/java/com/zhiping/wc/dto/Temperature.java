package com.zhiping.wc.dto;

import lombok.Data;
import lombok.ToString;

import java.math.BigDecimal;

@Data
@ToString
public class Temperature {
    private Long deviceId;
    private Integer deviceType;
    private BigDecimal temperature;
}
