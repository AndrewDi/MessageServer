package com.cmbchina;

/**
 * Created by Andrew on 29/12/2016.
 */
public class ColumnProperty {
    private String colName;
    private String colType;
    private int colNo;
    private int keySeq;
    private boolean nulls;
    private int length;

    public ColumnProperty(String colName, String colType, int colNo, int keySeq, boolean nulls, int length) {
        this.colName = colName;
        this.colType = colType;
        this.colNo = colNo;
        this.keySeq = keySeq;
        this.nulls = nulls;
        this.length = length;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public String getColType() {
        return colType;
    }

    public void setColType(String colType) {
        this.colType = colType;
    }

    public int getColNo() {
        return colNo;
    }

    public void setColNo(int colNo) {
        this.colNo = colNo;
    }

    public int getKeySeq() {
        return keySeq;
    }

    public void setKeySeq(int keySeq) {
        this.keySeq = keySeq;
    }

    public boolean isNulls() {
        return nulls;
    }

    public void setNulls(boolean nulls) {
        this.nulls = nulls;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }
}
