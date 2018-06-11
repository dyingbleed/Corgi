package com.dyingbleed.corgi.web.func;

/**
 * Created by 李震 on 2018/6/11.
 */
public class SyncTaskParam {

    private String hdfsMasterUrl;

    private String hdfsSlaveUrl;

    private String hiveMasterUrl;

    private String hiveMasterUsername;

    private String hiveMasterPassword;

    private String hiveSlaveUrl;

    private String hiveSlaveUsername;

    private String hiveSlavePassword;

    private String sinkDb;

    private String sinkTable;

    private Boolean isIncrement;

    public SyncTaskParam(String hdfsMasterUrl, String hdfsSlaveUrl, String hiveMasterUrl, String hiveMasterUsername, String hiveMasterPassword, String hiveSlaveUrl, String hiveSlaveUsername, String hiveSlavePassword, String sinkDb, String sinkTable) {
        this.hdfsMasterUrl = hdfsMasterUrl;
        this.hdfsSlaveUrl = hdfsSlaveUrl;
        this.hiveMasterUrl = hiveMasterUrl;
        this.hiveMasterUsername = hiveMasterUsername;
        this.hiveMasterPassword = hiveMasterPassword;
        this.hiveSlaveUrl = hiveSlaveUrl;
        this.hiveSlaveUsername = hiveSlaveUsername;
        this.hiveSlavePassword = hiveSlavePassword;
        this.sinkDb = sinkDb;
        this.sinkTable = sinkTable;

        this.isIncrement = false;
    }

    public String getHdfsMasterUrl() {
        return hdfsMasterUrl;
    }

    public void setHdfsMasterUrl(String hdfsMasterUrl) {
        this.hdfsMasterUrl = hdfsMasterUrl;
    }

    public String getHdfsSlaveUrl() {
        return hdfsSlaveUrl;
    }

    public void setHdfsSlaveUrl(String hdfsSlaveUrl) {
        this.hdfsSlaveUrl = hdfsSlaveUrl;
    }

    public String getHiveMasterUrl() {
        return hiveMasterUrl;
    }

    public void setHiveMasterUrl(String hiveMasterUrl) {
        this.hiveMasterUrl = hiveMasterUrl;
    }

    public String getHiveMasterUsername() {
        return hiveMasterUsername;
    }

    public void setHiveMasterUsername(String hiveMasterUsername) {
        this.hiveMasterUsername = hiveMasterUsername;
    }

    public String getHiveMasterPassword() {
        return hiveMasterPassword;
    }

    public void setHiveMasterPassword(String hiveMasterPassword) {
        this.hiveMasterPassword = hiveMasterPassword;
    }

    public String getHiveSlaveUrl() {
        return hiveSlaveUrl;
    }

    public void setHiveSlaveUrl(String hiveSlaveUrl) {
        this.hiveSlaveUrl = hiveSlaveUrl;
    }

    public String getHiveSlaveUsername() {
        return hiveSlaveUsername;
    }

    public void setHiveSlaveUsername(String hiveSlaveUsername) {
        this.hiveSlaveUsername = hiveSlaveUsername;
    }

    public String getHiveSlavePassword() {
        return hiveSlavePassword;
    }

    public void setHiveSlavePassword(String hiveSlavePassword) {
        this.hiveSlavePassword = hiveSlavePassword;
    }

    public String getSinkDb() {
        return sinkDb;
    }

    public void setSinkDb(String sinkDb) {
        this.sinkDb = sinkDb;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public Boolean isIncrement() {
        return isIncrement;
    }

    public void setIncrement(Boolean increment) {
        isIncrement = increment;
    }
}
