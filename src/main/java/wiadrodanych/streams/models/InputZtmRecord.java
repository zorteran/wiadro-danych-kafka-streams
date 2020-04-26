package wiadrodanych.streams.models;

import com.google.gson.annotations.SerializedName;

import java.util.Date;

public class InputZtmRecord {
    @SerializedName("Lines")
    public String lines;
    @SerializedName("Lon")
    public double lon;
    @SerializedName("Lat")
    public double lat;
    @SerializedName("VehicleNumber")
    public String vehicleNumber;
    @SerializedName("Brigade")
    public String brigade;
    @SerializedName("Time")
    public Date time;

    public InputZtmRecord() {
    }

    public InputZtmRecord(String lines, double lon, double lat, String vehicleNumber, String brigade, Date time) {
        this.lines = lines;
        this.lon = lon;
        this.lat = lat;
        this.vehicleNumber = vehicleNumber;
        this.brigade = brigade;
        this.time = time;
    }
}
