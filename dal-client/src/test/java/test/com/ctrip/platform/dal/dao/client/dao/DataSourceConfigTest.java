package test.com.ctrip.platform.dal.dao.client.dao;

import org.junit.Test;
import test.com.ctrip.platform.dal.dao.client.auto.DataSourceConfigDaoAuto;
import test.com.ctrip.platform.dal.dao.client.entity.DataSourceConfigPojo;

import java.sql.SQLException;

public class DataSourceConfigTest {
    @Test
    public void getOne() throws SQLException {
        DataSourceConfigDaoAuto auto=new DataSourceConfigDaoAuto();
        DataSourceConfigPojo result = auto.getOne(41);

    }
}
