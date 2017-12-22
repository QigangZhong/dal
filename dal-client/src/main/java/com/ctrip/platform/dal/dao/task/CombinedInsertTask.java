package com.ctrip.platform.dal.dao.task;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ctrip.platform.dal.dao.DalHintEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.helper.EntityManager;
import com.ctrip.platform.dal.exceptions.DalException;
import com.ctrip.platform.dal.exceptions.ErrorCode;

public class CombinedInsertTask<T> extends InsertTaskAdapter<T> implements BulkTask<Integer, T> {
	public static final String TMPL_SQL_MULTIPLE_INSERT = "INSERT INTO %s(%s) VALUES %s";

	@Override
	public Integer getEmptyValue() {
		return 0;
	}	

	@Override
	public BulkTaskContext<T> createTaskContext(DalHints hints, List<Map<String, ?>> daoPojos, List<T> rawPojos) {
		BulkTaskContext<T> context = new BulkTaskContext<T>(rawPojos);
		Set<String> unqualifiedColumns = filterUnqualifiedColumns(hints, daoPojos, rawPojos);
		context.setUnqualifiedColumns(unqualifiedColumns);
		return context;
	}

	@Override
	public Integer execute(DalHints hints, Map<Integer, Map<String, ?>> daoPojos, BulkTaskContext<T> taskContext) throws SQLException {
		StatementParameters parameters = new StatementParameters();
		StringBuilder values = new StringBuilder();

		Set<String> unqualifiedColumns = taskContext.getUnqualifiedColumns();
		
		List<String> finalInsertableColumns = buildValidColumnsForInsert(unqualifiedColumns);
		
		String insertColumns = combineColumns(finalInsertableColumns, COLUMN_SEPARATOR);
		
		int startIndex = 1;
		for (Integer index :daoPojos.keySet()) {
			Map<String, ?> pojo = daoPojos.get(index);
			
			removeUnqualifiedColumns(pojo, unqualifiedColumns);
			
			int paramCount = addParameters(startIndex, parameters, pojo, finalInsertableColumns);
			startIndex += paramCount;
			values.append(String.format("(%s),", combine("?", paramCount, ",")));
		}

		String sql = String.format(TMPL_SQL_MULTIPLE_INSERT,
				getTableName(hints), insertColumns,
				values.substring(0, values.length() - 2) + ")");

		KeyHolder keyHolder = hints.getKeyHolder();
		KeyHolder tmpHolder = keyHolder != null && keyHolder.isRequireMerge() ? new KeyHolder() : keyHolder;
		
		int count = client.update(sql, parameters, hints.setKeyHolder(tmpHolder));
		
		Integer[] indexList = daoPojos.keySet().toArray(new Integer[daoPojos.size()]);
		if(tmpHolder != null) {
			keyHolder.addPatial(indexList, tmpHolder);
		}
		
		hints.setKeyHolder(keyHolder);
		
		KeyHolder.insertKeyBack(hints, taskContext.getRawPojos(), indexList);
		
		return count;
	}

	@Override
	public BulkTaskResultMerger<Integer> createMerger() {
		return new ShardedIntResultMerger();
	}
	
	public static void insertKeyBack(DalHints hints, List<?> rawPojos, Integer[] indexList, KeyHolder keyHolder, String pkName) throws SQLException {
	    if(keyHolder == null)
	        return;
	    
	    if(!(hints.is(DalHintEnum.insertIdentityBack) && hints.isIdentityInsertDisabled()))
	        return;
	    
	    Class pojoClass = rawPojos.get(0).getClass();
        Field pkFlield = EntityManager.getEntityManager(pojoClass).getFieldMap().get(pkName);
        if(pkFlield == null)
            throw new IllegalArgumentException("insertIdentityBack only support JPA POJO. Please use code gen to regenerate your POJO");
        
        for(Integer index: indexList) {
            setPrimaryKey(pkFlield, rawPojos.get(index), keyHolder.getKey(index));
        }
	}
	
	/**
	 * Only support number type
	 * @throws SQLException
	 */
	private static void setPrimaryKey(Field pkFlield, Object entity, Number val) throws SQLException {
        try {
            if (pkFlield.getType().equals(Long.class) || pkFlield.getType().equals(long.class)) {
                pkFlield.set(entity, val.longValue());
                return;
            }
            if (pkFlield.getType().equals(Integer.class) || pkFlield.getType().equals(int.class)) {
                pkFlield.set(entity, val.intValue());
                return;
            }
            if (pkFlield.getType().equals(Byte.class) || pkFlield.getType().equals(byte.class)) {
                pkFlield.set(entity, val.byteValue());
                return;
            }
            if (pkFlield.getType().equals(Short.class) || pkFlield.getType().equals(short.class)) {
                pkFlield.set(entity, val.shortValue());
                return;
            }
        } catch (Throwable e) {
            throw new DalException(ErrorCode.SetPrimaryKeyFailed, entity.getClass().getName(), pkFlield.getName());
        }
    }
}