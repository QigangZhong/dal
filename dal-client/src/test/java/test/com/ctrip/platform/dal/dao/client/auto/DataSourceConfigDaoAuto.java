package test.com.ctrip.platform.dal.dao.client.auto;


import com.ctrip.platform.dal.common.enums.DatabaseCategory;
import com.ctrip.platform.dal.dao.*;
import com.ctrip.platform.dal.dao.helper.DalDefaultJpaMapper;
import com.ctrip.platform.dal.dao.helper.DalDefaultJpaParser;
import com.ctrip.platform.dal.dao.sqlbuilder.FreeSelectSqlBuilder;
import org.omg.CORBA.INTERNAL;
import test.com.ctrip.platform.dal.dao.client.entity.DataSourceConfigPojo;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class DataSourceConfigDaoAuto {

    private static final String DATA_BASE = "NewRetailDB";
    private static final DatabaseCategory dbCategory = DatabaseCategory.SqlServer;
    private DalQueryDao queryDao = null;
    private DalTableDao<DataSourceConfigPojo> updateDao = null;

    private DalRowMapper<DataSourceConfigPojo> dataSourceConfigPojoRowMapper = null;

    public DataSourceConfigDaoAuto() throws SQLException {
        this.dataSourceConfigPojoRowMapper = new DalDefaultJpaMapper<>(DataSourceConfigPojo.class);
        this.updateDao = new DalTableDao<>(new DalDefaultJpaParser<>(DataSourceConfigPojo.class,DATA_BASE));
        this.queryDao = new DalQueryDao(DATA_BASE);
    }

    /**
     * 根据门店获取数据源信息
     **/
    public DataSourceConfigPojo getOne(Integer storeSysNo) throws SQLException {
        return getOne(storeSysNo, null);
    }

    /**
     * 根据门店获取数据源信息
     **/
    public DataSourceConfigPojo getOne(Integer storeSysNo, DalHints hints) throws SQLException {
        hints = DalHints.createIfAbsent(hints);

        FreeSelectSqlBuilder<DataSourceConfigPojo> builder = new FreeSelectSqlBuilder<>(dbCategory);
        builder.setTemplate("select * from DataSourceConfig with (nolock) where Status=0 AND StoreSysNo=?");
        StatementParameters parameters = new StatementParameters();
        int i = 1;
        parameters.setSensitive(i++, "storeSysNo", Types.INTEGER, storeSysNo);
        builder.mapWith(dataSourceConfigPojoRowMapper).requireFirst().nullable();

        return (DataSourceConfigPojo)queryDao.query(builder, parameters, hints);
    }

    /**
     * 获取所有的数据源配置信息
     **/
    public List<DataSourceConfigPojo> getAll() throws SQLException {
        return getAll(null);
    }

    /**
     * 获取所有的数据源配置信息
     **/
    public List<DataSourceConfigPojo> getAll(DalHints hints) throws SQLException {
        hints = DalHints.createIfAbsent(hints);

        FreeSelectSqlBuilder<List<DataSourceConfigPojo>> builder = new FreeSelectSqlBuilder<>(dbCategory);
        builder.setTemplate("select * from DataSourceConfig with (nolock) where Status=0");
        StatementParameters parameters = new StatementParameters();
        builder.mapWith(dataSourceConfigPojoRowMapper);

        return queryDao.query(builder, parameters, hints);
    }

    public Integer insert(DataSourceConfigPojo entity) throws SQLException {
        KeyHolder keyHolder =new KeyHolder();
        int row = updateDao.insert(DalHints.createIfAbsent(null),keyHolder,entity);
        if(row>0){
            int sysNo = keyHolder.getKey(0).intValue();
            return sysNo;
        }else{
            return 0;
        }
    }

    public Integer update(DataSourceConfigPojo entity) throws SQLException {
        Integer rowNum = updateDao.update(DalHints.createIfAbsent(null),entity);
        return rowNum;
    }
}