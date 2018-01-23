package test.com.ctrip.platform.dal.dao.client.entity;


import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.platform.dal.dao.annotation.Database;
import com.ctrip.platform.dal.dao.annotation.Type;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;
import java.sql.Types;

@Entity
@Database(name="NewRetailDB")
@Table(name="DataSourceConfig")
public class DataSourceConfigPojo implements DalPojo {

    @Id
    @Column(name="SysNo")
    @Type(value=Types.INTEGER)
    private Integer sysNo;

    @Column(name="StoreSysNo")
    @Type(value=Types.INTEGER)
    private Integer storeSysNo;

    @Column(name="DataSourceName")
    @Type(value=Types.VARCHAR)
    private String dataSourceName;

    @Column(name="IdcNetServiceUrl")
    @Type(value=Types.VARCHAR)
    private String idcNetServiceUrl;

    @Column(name="Remark")
    @Type(value=Types.NVARCHAR)
    private String remark;

    @Column(name="Status")
    @Type(value=Types.INTEGER)
    private Integer status;

    @Column(name="CreateTime")
    @Type(value=Types.TIMESTAMP)
    private Timestamp createTime;

    @Column(name="LastUpdateTime")
    @Type(value=Types.TIMESTAMP)
    private Timestamp lastUpdateTime;

    public Integer getSysNo() {
        return sysNo;
    }

    public void setSysNo(Integer sysNo) {
        this.sysNo = sysNo;
    }

    public Integer getStoreSysNo() {
        return storeSysNo;
    }

    public void setStoreSysNo(Integer storeSysNo) {
        this.storeSysNo = storeSysNo;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public String getIdcNetServiceUrl() {
        return idcNetServiceUrl;
    }

    public void setIdcNetServiceUrl(String idcNetServiceUrl) {
        this.idcNetServiceUrl = idcNetServiceUrl;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    public Timestamp getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Timestamp lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
}