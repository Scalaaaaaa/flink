package com.zhiping.wc.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.java.utils.ParameterTool;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class DeviceAvgTemperature {
    private Long id;

    private BigDecimal avgTemperature;

    private String websocketId;
    private String content;

}
