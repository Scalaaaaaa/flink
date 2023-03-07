package com.zhiping.wc.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class JustAvgTemperature {
    private BigDecimal avgTemperature;
}
