package wiadrodanych.streams.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.gson.annotations.SerializedName;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

public class OutputZtmRecord {
    public String lines;
    public double lon;
    public double lat;
    public String vehicleNumber;
    public String brigade;
    public Date time;
    public double speed;
    public double rotation;
    public double distance;

    @JsonIgnore
    public boolean toBeDeleted = false;

    public OutputZtmRecord() {
    }

    public OutputZtmRecord(InputZtmRecord record) {
        this.lines = record.lines;
        this.lon = record.lon;
        this.lat = record.lat;
        this.vehicleNumber = record.vehicleNumber;
        this.brigade = record.brigade;
        this.time = record.time;
    }
}
