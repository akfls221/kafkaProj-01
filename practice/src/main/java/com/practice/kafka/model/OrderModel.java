package com.practice.kafka.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public class OrderModel implements Serializable {
    public String orderId;
    public String shopId;
    public String menuName;
    public String userName;
    public String phoneNumber;
    public String address;
    public LocalDateTime orderTime;

    public OrderModel() {
    }

    public OrderModel(String orderId, String shopId, String menuName, String userName,
                      String phoneNumber, String address, LocalDateTime orderTime) {
        this.orderId = orderId;
        this.shopId = shopId;
        this.menuName = menuName;
        this.userName = userName;
        this.phoneNumber = phoneNumber;
        this.address = address;
        this.orderTime = orderTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getShopId() {
        return shopId;
    }

    public String getMenuName() {
        return menuName;
    }

    public String getUserName() {
        return userName;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public String getAddress() {
        return address;
    }

    public LocalDateTime getOrderTime() {
        return orderTime;
    }

    @Override
    public String toString() {
        return "OrderModel{" +
                "orderId='" + orderId + '\'' +
                ", shopId='" + shopId + '\'' +
                ", menuName='" + menuName + '\'' +
                ", userName='" + userName + '\'' +
                ", phoneNumber='" + phoneNumber + '\'' +
                ", address='" + address + '\'' +
                ", orderTime=" + orderTime +
                '}';
    }
}
