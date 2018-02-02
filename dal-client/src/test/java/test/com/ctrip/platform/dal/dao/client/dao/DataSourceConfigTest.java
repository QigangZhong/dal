package test.com.ctrip.platform.dal.dao.client.dao;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import test.com.ctrip.platform.dal.dao.client.auto.DataSourceConfigDaoAuto;
import test.com.ctrip.platform.dal.dao.client.entity.DataSourceConfigPojo;

import java.sql.SQLException;
import java.sql.Timestamp;

public class DataSourceConfigTest {
    private DataSourceConfigDaoAuto auto=null;
    @Before
    public void initDaoAuto() throws SQLException {
        auto=new DataSourceConfigDaoAuto();
    }

    @Test
    public void getOne() throws SQLException {
        DataSourceConfigPojo result = auto.getOne(999);
        System.out.println(result.getDataSourceName());
    }

    @Test
    public void insert() throws SQLException{
        DataSourceConfigPojo entity=new DataSourceConfigPojo();
        entity.setStoreSysNo(999);
        entity.setDataSourceName("testdb");
        //entity.setIdcNetServiceUrl("");
        entity.setStatus(0);
        //entity.setRemark("testdb remark");
        Timestamp now=new Timestamp(System.currentTimeMillis());
        entity.setCreateTime(now);
        entity.setLastUpdateTime(now);
        Integer sysNo = auto.insert(entity);
        System.out.println(sysNo);
    }

    @Test
    public void update() throws SQLException{
        DataSourceConfigDaoAuto auto=new DataSourceConfigDaoAuto();
        DataSourceConfigPojo result = auto.getOne(999);

        result.setRemark("99999999");
        Integer rowNum = auto.update(result);
        System.out.println(rowNum);
    }
}
