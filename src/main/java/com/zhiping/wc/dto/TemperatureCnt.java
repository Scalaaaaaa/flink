package com.zhiping.wc.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TemperatureCnt {
    private BigDecimal total;
    private Long cnt;
}
