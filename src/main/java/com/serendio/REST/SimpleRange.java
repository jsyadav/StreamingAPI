package com.serendio.REST;

public class SimpleRange {
    private String field;
    private String minValue;
    private String maxValue;

    public SimpleRange(String field,String minVal,String maxVal){
            this.field = field;
            this.minValue=minVal;
            this.maxValue = maxVal;
    }

    public String getMinValue() {
            return minValue;
    }

    public void setMinValue(String minValue) {
            this.minValue = minValue;
    }

    public String getMaxValue() {
            return maxValue;
    }

    public void setMaxValue(String maxValue) {
            this.maxValue = maxValue;
    }

    public String getField() {
            return field;
    }

    public void setField(String field) {
            this.field = field;
    }



}
