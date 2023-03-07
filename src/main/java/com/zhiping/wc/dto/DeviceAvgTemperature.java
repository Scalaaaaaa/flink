package com.zhiping.wc.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class DeviceAvgTemperature {
    private Long id;

    private BigDecimal avgTemperature;
}
