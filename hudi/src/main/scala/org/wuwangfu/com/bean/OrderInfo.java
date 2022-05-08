package org.wuwangfu.com.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderInfo {

    private Long orderId;
    private Integer productId;
    private String cityId;
    private String district;
    private String county;
    /**
     * 订单类型
     */
    private Integer type;
    /**
     * 订单时效性
     */
    private Integer comboType;
    /**
     * 交通类型
     */
    private Integer trafficType;
    /**
     * 乘车人数
     */
    private Integer passengerCount;
    /**
     * 司机子产品线
     */
    private Integer driverProductId;
    /**
     * 乘客发单时出发地与终点的预估路面距离
     */
    private Double startDestDistance;
    /**
     * 司机点击‘到达’的时间
     */
    private String arriveTime;
    /**
     * 出发时间
     */
    private String departureTime;
    /**
     * 预估价格
     */
    private Double preTotalFee;
    /**
     * 时长
     */
    private String normalTime;
    /**
     * 一级业务线
     */
    private Integer productLevel;
    /**
     * 终点经度
     */
    private Double destLng;
    /**
     * 终点纬度
     */
    private Double destLat;
    /**
     * 起点经度
     */
    private Double startingLng;
    /**
     * 起点纬度
     */
    private Double startingLat;
    private Integer year;
    private Integer month;
    private Integer day;

}
