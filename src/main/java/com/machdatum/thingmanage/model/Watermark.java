package com.machdatum.thingmanage.model;

public class Watermark extends Column{
    public int Interval;
    public TimeUnit Unit;

    public Watermark(String source, String type, String as, int interval, TimeUnit unit) {
        super(source, type, as);
        this.Interval = interval;
        this.Unit = unit;
    }
    public Watermark(String source, String type, String as) {
        super(source, type, as);
    }
    public int getInterval(){
        return this.Interval;
    }
    public void setInterval(int interval){
        this.Interval = interval;
    }
    public TimeUnit getUnit(){
        return this.Unit;
    }
    public void setUnit(TimeUnit unit){
        this.Unit = unit;
    }
}
