package test.com.ctrip.platform.dal.dao.client.dao;

import org.junit.Test;
import test.com.ctrip.platform.dal.dao.client.auto.DataSourceConfigDaoAuto;
import test.com.ctrip.platform.dal.dao.client.entity.DataSourceConfigPojo;

import java.sql.SQLException;
import java.sql.Timestamp;

public class DataSourceConfigTest {
    @Test
    public void getOne() throws SQLException {
        DataSourceConfigDaoAuto auto=new DataSourceConfigDaoAuto();
        DataSourceConfigPojo result = auto.getOne(41);
    }

    @Test
    public void insert() throws SQLException{
        DataSourceConfigDaoAuto auto=new DataSourceConfigDaoAuto();
        DataSourceConfigPojo entity=new DataSourceConfigPojo();
        entity.setStoreSysNo(999);
        entity.setDataSourceName("testdb");
        entity.setIdcNetServiceUrl("");
        entity.setStatus(0);
        entity.setRemark("testdb remark");
        Timestamp now=new Timestamp(System.currentTimeMillis());
        entity.setCreateTime(now);
        entity.setLastUpdateTime(now);
        Integer sysNo = auto.insert(entity);
    }
}
