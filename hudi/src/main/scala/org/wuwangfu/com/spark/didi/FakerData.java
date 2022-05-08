package org.wuwangfu.com.spark.didi;

import com.github.javafaker.Faker;
import org.wuwangfu.com.bean.OrderInfo;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Random;

public class FakerData {

    public static void main(String[] args) {


        Faker faker = new Faker(Locale.CHINA);
        Random random = new Random();

        OrderInfo orderInfo = new OrderInfo();
        orderInfo.setOrderId(faker.random().nextLong(10000));
        orderInfo.setProductId(Integer.valueOf(random.nextInt(4)-0));
        orderInfo.setCityId(faker.address().city());
        orderInfo.setDistrict(faker.address().buildingNumber());
        orderInfo.setCounty(faker.address().country());
        orderInfo.setType(Integer.valueOf(random.nextInt(3)-0));//生成[0,3]区间的整数
        orderInfo.setComboType(Integer.valueOf(random.nextInt(3)-0));
        orderInfo.setTrafficType(Integer.valueOf(random.nextInt(3)-0));
        orderInfo.setPassengerCount(Integer.valueOf(random.nextInt(100)-0));
        orderInfo.setDriverProductId(faker.random().nextInt(1));
        orderInfo.setStartDestDistance(Math.random()*90+10);//[10,100]
        orderInfo.setArriveTime(getDate());
        orderInfo.setDepartureTime(getDate());
        orderInfo.setPreTotalFee(Math.random()*200+10);
        orderInfo.setNormalTime(String.valueOf(random.nextInt(60)-10));
        orderInfo.setProductLevel(Integer.valueOf(random.nextInt(3)-0));
        orderInfo.setDestLng(Math.random()*73+62);//经度范围：73°33′E至135°05′E
        orderInfo.setDestLat(Math.random()*73+62);
        orderInfo.setStartingLng(Math.random()*3+50);//纬度范围：3°51′N至53°33′N
        orderInfo.setStartingLat(Math.random()*3+50);
        orderInfo.setYear(2022);
        orderInfo.setMonth(Integer.valueOf(random.nextInt(11)+1));
        orderInfo.setDay(Integer.valueOf(random.nextInt(30)+1));

        System.out.println(orderInfo);
    }


    public static String getDate(){
        //获取日历对象
        Calendar ca = Calendar.getInstance();
        //设置时间格式
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd kk:mm");

        //设置开始时间为当前时间3分钟以后
        ca.add(Calendar.DAY_OF_MONTH, +1);
        ca.add(Calendar.MINUTE, +2);
        Date startDate = ca.getTime();
        String startTime = sdf.format(startDate);

        //设置结束时间为1小时以后
        ca.add(Calendar.HOUR_OF_DAY, +1);
        Date endDate = ca.getTime();
        String endTime = sdf.format(endDate);

//        System.out.println(startTime + "_" + endTime);
        return startTime;
    }
}
