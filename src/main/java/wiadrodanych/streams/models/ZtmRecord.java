package wiadrodanych.streams.models;

import java.util.Date;

public class ZtmRecord {
    public String lines;
    public double lon;
    public double lat;
    public String vehicleNumber;
    public String brigade;
    public Date time;
    public double speed;
    public double distance;
    public double bearing;

    public ZtmRecord() {
    }

    public ZtmRecord(InputZtmRecord record) {
        this.lines = record.lines;
        this.lon = record.lon;
        this.lat = record.lat;
        this.vehicleNumber = record.vehicleNumber;
        this.brigade = record.brigade;
        this.time = record.time;
    }
}
