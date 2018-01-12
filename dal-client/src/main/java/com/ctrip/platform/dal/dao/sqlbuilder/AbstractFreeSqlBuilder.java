package com.ctrip.platform.dal.dao.sqlbuilder;

import static com.ctrip.platform.dal.dao.helper.DalShardingHelper.buildShardStr;
import static com.ctrip.platform.dal.dao.helper.DalShardingHelper.isTableShardingEnabled;
import static com.ctrip.platform.dal.dao.helper.DalShardingHelper.locateTableShardId;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractTableSqlBuilder.wrapField;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import com.ctrip.platform.dal.common.enums.DatabaseCategory;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.sqlbuilder.Expressions.ColumnExpression;
import com.ctrip.platform.dal.dao.sqlbuilder.Expressions.Expression;

/**
 * This sql builder only handles template creation. It will not do with the parameters
 * for now.
 * 
 * rules:
 * if bracket has no content, bracket will be removed
 * expression can be evaluated and can be wrapped by bracket and connect to each other by and/or
 * expression should have no leading and tailing and/or, it there is, the and/or will be removed during validating
 * 
 * To be align with FreeSelectSqlBuilder and FreeUpdateSqlBuilder, this class will not handle methods that dealing with 
 * some parameter. To deal with parameter, you should use Expressions.*
 * 
 * @author jhhe
 *
 */
public class AbstractFreeSqlBuilder extends AbstractSqlBuilder {
    /**
     * Because builder will skip space for certain case, 
     * you can append EMPTY to bypass this restriction. 
     */
    public static final String EMPTY = "";
    public static final String PLACE_HOLDER = "?";
    
    /**
     * Builder will not insert space before COMMA.
     */
    public static final Text COMMA = text(",");
    
    public static final Text SPACE = text(" ");
    
    public static final Keyword SELECT = keyword("SELECT");
    public static final Keyword FROM = keyword("FROM");
    public static final Keyword WHERE= keyword("WHERE");
    public static final Keyword AS = keyword("AS");
    public static final Keyword ORDER_BY = keyword("ORDER BY");
    public static final Keyword ASC = keyword("ASC");
    public static final Keyword DESC = keyword("DESC");
    public static final Keyword GROUP_BY = keyword("GROUP BY");
    public static final Keyword HAVING = keyword("HAVING");
    
    private BuilderContext context;
    
    public AbstractFreeSqlBuilder() {
        super(new BuilderContext());
        context = getContext();
    }
    
    /**
     * In case there is Table clause appended, logic DB must be set to determine
     * if the table name can be sharded or not. Set this logic db name will also
     * set db category identified by the logic db name. So you don't need to set
     * db category again.
     * 
     * @param logicDbName
     * @return
     */
    public AbstractFreeSqlBuilder setLogicDbName(String logicDbName) {
        context.setLogicDbName(logicDbName);
        return this;
    }
    
    /**
     * If you already set logic db name, then you don't need to set this.
     *  
     * @param dbCategory
     * @return
     */
    public AbstractFreeSqlBuilder setDbCategory(DatabaseCategory dbCategory) {
        context.setDbCategory(dbCategory);
        return this;
    }
    
    public AbstractFreeSqlBuilder setHints(DalHints hints) {
        context.setHints(hints);
        return this;
    }
    
    /**
     * Specify parameters that come with this builder
     * @param parameters
     * @return
     */
    public AbstractFreeSqlBuilder with(StatementParameters parameters) {
        context.setParameters(parameters);
        return this;
    }
    
    @Override
    public StatementParameters buildParameters() {
        return context.getParameters().buildParameters();
    }
    
    public static Text text(String template) {
        return new Text(template);
    }
    
    public static Clause texts(String...templates) {
        ClauseList clauses = new ClauseList();
        for (int i = 0; i < templates.length; i++) {
            clauses.add(column(templates[i]));
        }
        
        return clauses;
    }
    
    public static Keyword keyword(String keyword) {
        return new Keyword(keyword);
    }
    
    /**
     * Create Column clause with given name
     * @param columnName
     * @return
     */
    public static Column column(String columnName) {
        return new Column(columnName);
    }
    
    /**
     * Turn string array into column clauses
     * 
     * @param columnNames
     * @return
     */
    public static Clause columns(String... columnNames) {
        ClauseList clauses = new ClauseList();
        for (int i = 0; i < columnNames.length; i++) {
            clauses.add(column(columnNames[i]));
        }
        
        return clauses;
    }
    
    /**
     * Create Table clause with given name
     * @param tableName
     * @return
     */
    public static Table table(String tableName) {
        return new Table(tableName);
    }
    
    /**
     * Add parameter methods definition
     */
    
    private int nextIndex() {
        return context.getParameters().nextIndex();
    }
    
    /**
     * Set parameter
     * @param sqlType java.sql.Types
     */
    public AbstractFreeSqlBuilder set(String name, int sqlType, Object value) {
        context.getParameters().set(nextIndex(), name, sqlType, value);
        return this;
    }
    
    /**
     * Set parameter when value is not null
     * @param sqlType java.sql.Types
     */
    public AbstractFreeSqlBuilder setNullable(String name, int sqlType, Object value) {
        return set(value != null, name, sqlType, value);
    }
    
    /**
     * Set parameter when condition is satisfied
     * @param sqlType java.sql.Types
     */
    public AbstractFreeSqlBuilder set(boolean condition, String name, int sqlType, Object value) {
        set(name, sqlType, value);
        getParameters().when(condition);
        return this;
    }
    
    /**
     * Set in parameter
     * @param sqlType java.sql.Types
     */
    public AbstractFreeSqlBuilder setIn(String name, int sqlType, List<?> values) throws SQLException {
        context.getParameters().setInParameter(nextIndex(), name, sqlType, values);
        return this;
    }
    
    /**
     * Set in parameter when value is not null and all its elements are not null
     * @param sqlType java.sql.Types
     */
    public AbstractFreeSqlBuilder setInNullable(String name, int sqlType, List<?> values) throws SQLException {
        setIn(name, sqlType, values);
        getParameters().nullable();
        return this;
    }
    
    /**
     * Set in parameter when condition is satisfied
     * @param sqlType java.sql.Types
     */
    public AbstractFreeSqlBuilder setIn(boolean condition, String name, int sqlType, List<?> values) throws SQLException {
        setIn(name, sqlType, values);
        getParameters().when(condition);
        return this;
    }
    
    /**
     * Basic append methods definition
     */
    
    /**
     * Basic append method. Parameter value can be String, Clause or Object. It will allow the maximal
     * flexibility for input parameter.
     * 
     * @param template String
     * @return builder itself
     */
    public AbstractFreeSqlBuilder append(String template) {
        Objects.requireNonNull(template, "Parameter template is NULL.");
        return append(text(template));
    }
    
    /**
     * Append multiple template to the builder. Parameter of String will be append as Text.
     *  
     * If used with Expressions static methods, you can build
     * sql in a very flexible way. Usage like:
     * 
     * append(
     *          text("orderId > ?")
     *          AND,
     *          leftBracket,
     *          NOT, equals("Abc"),
     *          expression("count(1)"),
     *          rightBracket,
     *          OR,
     *          ...
     *       )
     * @param templates
     * @return
     */
    public AbstractFreeSqlBuilder append(Clause... templates) {
        for(Clause template: templates) {
            add(template);
        }

        return this;
    }
    
    /**
     * Append when the condition is met
     * @param condition
     * @param template
     * @return
     */
    public AbstractFreeSqlBuilder appendWhen(boolean condition, String template) {
        return appendWhen(condition, text(template));
    }
    
    /**
     * Append when the condition is met
     * @param condition
     * @param template
     * @return
     */
    public AbstractFreeSqlBuilder appendWhen(boolean condition, Clause template) {
        return condition ? append(template): this;
    }
    
    /**
     * Append template depends on whether the condition is met.
     * @param condition
     * @param template value to be appended when condition is true
     * @param elseTemplate value to be appended when condition is true
     * @return
     */
    public AbstractFreeSqlBuilder appendWhen(boolean condition, String template, String elseTemplate) {
        return appendWhen(condition, text(template), text(elseTemplate));
    }
    
    /**
     * Append template depends on whether the condition is met.
     * @param condition
     * @param template value to be appended when condition is true
     * @param elseTemplate value to be appended when condition is true
     * @return
     */
    public AbstractFreeSqlBuilder appendWhen(boolean condition, Clause template, Clause elseTemplate) {
        return condition ? append(template): append(elseTemplate);
    }
    
    /**
     * Append as column. The column name will be quoted by database specific char.
     * 
     * @param columnNames 
     * @return
     */
    public AbstractFreeSqlBuilder appendColumn(String columnName) {
        return append(column(columnName));
    }
    
    /**
     * Append as column with alias. The column name will be quoted by database specific char.
     * 
     * @param columnNames 
     * @param alias
     * @return
     */
    public AbstractFreeSqlBuilder appendColumn(String columnName, String alias) {
        return append(column(columnName).as(alias));
    }
    
    /**
     * Append as Table. Same as append(table(tableName)).
     * 
     * The tableName will be replaced by true table name if it is a logic table that allow shard.
     * 
     * @param tableName table name. The table can be sharded
     * @return
     */
    public AbstractFreeSqlBuilder appendTable(String tableName) {
        return append(table(tableName));
    }
    
    /**
     * Append as Table with alias. Same as append(table(tableName)).
     * 
     * The tableName will be replaced by true table name if it is a logic table that allow shard.
     * 
     * @param tableName table name. The table can be sharded
     * @param alias
     * @return
     */
    public AbstractFreeSqlBuilder appendTable(String tableName, String alias) {
        return append(table(tableName).as(alias));
    }
    
    /**
     * Append as Expression. Same as append(expression(expression))
     * 
     * @param expression
     * @return
     */
    public AbstractFreeSqlBuilder appendExpression(String expression) {
        return append(new Expression(expression));
    }

    /**
     * Combined append for SELECT
     */

    /**
     * Build a SELECT column1, column2,...using the giving columnNames
     * 
     * Note: The String parameter will be wrapped by Column clause.
     * 
     * @param columns The type of column can be Column or other clause
     * @param table
     * @return
     */
    public AbstractFreeSqlBuilder select(String... columnNames) {
        return select(columns(columnNames));
    }
    
    public AbstractFreeSqlBuilder select(Clause... columnNames) {
        append(SELECT);
        ClauseList clauses = new ClauseList();
        clauses.add(columnNames);
        int size = clauses.getList().size();
        for (int i = 0; i < size; i++) {
            append(columnNames[i]);
            if(i != size -1)
                append(COMMA);    
        }

        return this;
    }
    
    /**
     * Append SELECT *
     * 
     * @return
     */
    public AbstractFreeSqlBuilder selectAll() {
        return append(SELECT).append("*");
    }
    
    /**
     * Append FROM and table for SELECT statement. And if logic DB is sql server, it will 
     * append "WITH (NOLOCK)" by default 
     * 
     * @param columns The type of column can be Column or other clause
     * @param table table name string
     * @return
     */
    public AbstractFreeSqlBuilder from(String table) {
        return from(table(table));
    }
    
    /**
     * Append FROM and table for query. And if logic DB is sql server, it will 
     * append "WITH (NOLOCK)" by default 
     * 
     * @param columns The type of column can be Column or other clause
     * @param table table name clause
     * @return
     */
    public AbstractFreeSqlBuilder from(Table table) {
        return append(FROM).append(table).append(new SqlServerWithNoLock());
    }
    
    /**
     * Append WHERE alone with expressions.
     * 
     * If you want to append 1=1 at the beginning, please use where(includeAll()) 
     * or if you want to select nothing if there is no valid record, please 
     * use where(excludeAll());
     * 
     * @param expressions
     * @return
     */
    public AbstractFreeSqlBuilder where(Clause...expressions) {
        return append(WHERE).append(expressions);
    }
    
    /**
     * @return exprerssion TRUE and AND
     */
    public static Object[] includeAll() {
        return new Object[]{Expressions.TRUE, Expressions.AND};
    }
    
    /**
     * @return exprerssion FALSE and OR
     */
    public static Object[] excludeAll() {
        return new Object[]{Expressions.FALSE, Expressions.OR};
    }
    
    /**
     * Append ORDER BY with column name
     * 
     * @param columnName
     * @param ascending
     * @return
     */
    public AbstractFreeSqlBuilder orderBy(String columnName, boolean ascending){
        return append(ORDER_BY).append(column(columnName)).appendWhen(ascending, ASC, DESC);
    }
    
    /**
     * Append GROUP BY with column name
     * 
     * @param columnName
     * @return
     */
    public AbstractFreeSqlBuilder groupBy(String columnName) {
        return append(GROUP_BY).append(column(columnName));
    }
    
    public AbstractFreeSqlBuilder groupBy(Clause condition) {
        return append(GROUP_BY).append(condition);
    }
    
    /**
     * Append HAVING with condition
     * 
     * @param condition
     * @return
     */
    public AbstractFreeSqlBuilder having(String condition) {
        return append(HAVING).append(condition);
    }
    
    public AbstractFreeSqlBuilder leftBracket() {
        return append(Expressions.leftBracket);
    }

    public AbstractFreeSqlBuilder rightBracket() {
        return append(Expressions.rightBracket);
    }
    
    /**
     * Append multiple expression into ().
     * 
     * @param expressions
     * @return
     */
    public AbstractFreeSqlBuilder bracket(Clause... expressions) {
        return leftBracket().append(expressions).rightBracket();
    }
    
    public AbstractFreeSqlBuilder and() {
        return append(Expressions.AND);
    }
    
    public AbstractFreeSqlBuilder or() {
        return append(Expressions.OR);
    }
    
    public AbstractFreeSqlBuilder not() {
        return append(Expressions.NOT);
    }
    
    /**
     * Join multiple expression with AND.
     * 
     * @param expressions
     * @return
     */
    public AbstractFreeSqlBuilder and(Clause... expressions) {
        for (int i = 0; i < expressions.length; i++) {
            append(expressions[i]);
            if(i != expressions.length -1)
                and();    
        }
        
        return this;
    }

    /**
     * Join multiple expression with OR.
     * 
     * @param expressions
     * @return
     */
    public AbstractFreeSqlBuilder or(Clause... expressions) {
        for (int i = 0; i < expressions.length; i++) {
            append(expressions[i]);
            if(i != expressions.length -1)
                or();    
        }
        
        return this;
    }
    
    /**
     * Mark last expression as valid expression when value is not null.
     * @param value
     * @return
     */
    public AbstractFreeSqlBuilder nullable(Object value) {
        getLastExpression().nullable(value);
        return this;
    }
    
    /**
     * Mark last ColumnExpression as valid expression when value is not null.
     * @param value
     * @return
     */
    public AbstractFreeSqlBuilder nullable() {
        Expression last = getLastExpression();

        if(!(last instanceof ColumnExpression))
            throw new IllegalStateException("The last sql segement is not an ColumnExpression.");
        
        ((ColumnExpression)last).nullable();
        return this;    
    }

    /**
     * Mark last expression as valid expression that is to be used in builder when the condition is met.
     * @param value
     * @return
     */
    public AbstractFreeSqlBuilder when(boolean condition) {
        getLastExpression().when(condition);
        return this;
    }
    
    private Expression getLastExpression() {
        List<Clause> list = getClauseList().getList();
        
        if(list.isEmpty())
            throw new IllegalStateException("There is no exitsing sql segement.");
        
        Clause last = list.get(list.size() - 1);
        
        if(!(last instanceof Expression))
            throw new IllegalStateException("The last sql segement is not an expression.");
        
        return ((Expression)last);
    }

    /**
     * Append = expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder equal(String columnName) {
        return append(Expressions.equal(columnName));
    }
    
    /**
     * Append = expression using the giving name, type and value
     * @param columnName  column name, can not be expression.
     * @param sqlType corresponding sql type of the value
     * @param value the value of the expression
     * @return
     */
    public AbstractFreeSqlBuilder equal(String columnName, int sqlType, Object value) {
        return append(Expressions.equal(columnName, sqlType, value));
    }
    
    /**
     * Append <> expression using the giving name, type and value
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder notEqual(String columnName) {
        return append(Expressions.notEqual(columnName));
    }
    
    /**
     * Append <> expression using the giving name, type and value
     * @param columnName  column name, can not be expression.
     * @param sqlType corresponding sql type of the value
     * @param value the value of the expression@return
     */
    public AbstractFreeSqlBuilder notEqual(String columnName, int sqlType, Object value) {
        return append(Expressions.notEqual(columnName, sqlType, value));
    }
    
    /**
     * Append > expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder greaterThan(String columnName) {
        return append(Expressions.greaterThan(columnName));
    }

    /**
     * Append > expression using the giving name, type and value
     * @param columnName  column name, can not be expression.
     * @param sqlType corresponding sql type of the value
     * @param value the value of the expression@return
     * @return
     */
    public AbstractFreeSqlBuilder greaterThan(String columnName, int sqlType, Object value) {
        return append(Expressions.greaterThan(columnName, sqlType, value));
    }

    /**
     * Append >= expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder greaterThanEquals(String columnName) {
        return append(Expressions.greaterThanEquals(columnName));
    }

    /**
     * Append >= expression using the giving name, type and value
     * @param columnName  column name, can not be expression.
     * @param sqlType corresponding sql type of the value
     * @param value the value of the expression
     * @return
     */
    public AbstractFreeSqlBuilder greaterThanEquals(String columnName, int sqlType, Object value) {
        return append(Expressions.greaterThanEquals(columnName, sqlType, value));
    }

    /**
     * Append < expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder lessThan(String columnName) {
        return append(Expressions.lessThan(columnName));
    }

    /**
     * Append < expression using the giving name, type and value
     * @param columnName  column name, can not be expression.
     * @param sqlType corresponding sql type of the value
     * @param value the value of the expression
     * @return
     */
    public AbstractFreeSqlBuilder lessThan(String columnName, int sqlType, Object value) {
        return append(Expressions.lessThan(columnName, sqlType, value));
    }

    /**
     * Append <= expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder lessThanEquals(String columnName) {
        return append(Expressions.lessThanEquals(columnName));
    }

    /**
     * Append <= expression using the giving name, type and value
     * @param columnName  column name, can not be expression.
     * @param sqlType corresponding sql type of the value
     * @param value the value of the expression
     * @return
     */
    public AbstractFreeSqlBuilder lessThanEquals(String columnName, int sqlType, Object value) {
        return append(Expressions.lessThanEquals(columnName, sqlType, value));
    }

    /**
     * Append BETWEEN expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder between(String columnName) {
        return append(Expressions.between(columnName));
    }
    
    /**
     * Append BETWEEN expression using the giving name, type and value
     * @param columnName  column name, can not be expression.
     * @param sqlType corresponding sql type of the value
     * @param value the value of the expression
     * @return
     */
    public AbstractFreeSqlBuilder between(String columnName, int sqlType, Object lowerValue, Object upperValue) {
        return append(Expressions.between(columnName, sqlType, lowerValue, upperValue));
    }
    
    /**
     * Append NOT BETWEEN expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder notBetween(String columnName) {
        return append(Expressions.notBetween(columnName));
    }
    
    /**
     * Append NOT BETWEEN expression using the giving name, type and value
     * @param columnName  column name, can not be expression.
     * @param sqlType corresponding sql type of the value
     * @param value the value of the expression
     * @return
     */
    public AbstractFreeSqlBuilder notBetween(String columnName, int sqlType, Object lowerValue, Object upperValue) {
        return append(Expressions.notBetween(columnName, sqlType, lowerValue, upperValue));
    }
    
    /**
     * Append LIKE expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder like(String columnName) {
        return append(Expressions.like(columnName));
    }
    
    /**
     * Append LIKE expression using the giving name, type and value
     * @param columnName  column name, can not be expression.
     * @param sqlType corresponding sql type of the value
     * @param value the value of the expression
     * @return
     */
    public AbstractFreeSqlBuilder like(String columnName, int sqlType, Object value) {
        return append(Expressions.like(columnName, sqlType, value));
    }

    /**
     * Append BOT LIKE expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder notLike(String columnName) {
        return append(Expressions.notLike(columnName));
    }
    
    /**
     * Append BOT LIKE expression using the giving name, type and value
     * @param columnName  column name, can not be expression.
     * @param sqlType corresponding sql type of the value
     * @param value the value of the expression
     * @return
     */
    public AbstractFreeSqlBuilder notLike(String columnName, int sqlType, Object value) {
        return append(Expressions.notLike(columnName, sqlType, value));
    }
    
    /**
     * Append IN expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder in(String columnName) {
        return append(Expressions.in(columnName));
    }
    
    /**
     * Append IN expression using the giving name, type and value
     * @param columnName  column name, can not be expression.
     * @param sqlType corresponding sql type of the value
     * @param value the value of the expression
     * @return
     */
    public AbstractFreeSqlBuilder in(String columnName, int sqlType, Collection<?> values) {
        return append(Expressions.in(columnName, sqlType, values));
    }
    
    /**
     * Append NOT IN expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder notIn(String columnName) {
        return append(Expressions.notIn(columnName));
    }
    
    /**
     * Append NOT IN expression using the giving name, type and value
     * @param columnName  column name, can not be expression.
     * @param sqlType corresponding sql type of the value
     * @param value the value of the expression
     * @return
     */
    public AbstractFreeSqlBuilder notIn(String columnName, int sqlType, Collection<?> values) {
        return append(Expressions.notIn(columnName, sqlType, values));
    }
    
    /**
     * Append IS NULL expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder isNull(String columnName) {
        return append(Expressions.isNull(columnName));
    }
    
    /**
     * Append IS NOT NULL expression using the giving name
     * @param columnName  column name, can not be expression.
     * @return
     */
    public AbstractFreeSqlBuilder isNotNull(String columnName) {
        return append(Expressions.isNotNull(columnName));
    }
    
    public static class Text extends Clause {
        private String template;
        public Text(String template) {
            this.template =template;
        }
        
        public String build() {
            return template;
        }
        
        public String toString() {
            return build();
        }
    }
    
    public static class Keyword extends Text {
        public Keyword(String keyword) {
            super(keyword);
        }
    }

    public static class Column extends Clause {
        private String columnName;
        private String alias;
        public Column(String columnName) {
            this.columnName = columnName;
        }
        
        public Column(String columnName, String alias) {
            this(columnName);
            this.alias = alias;
        }
        
        public Column as(String alias) {
            this.alias = alias;
            return this;
        }
        
        public String build() {
            return alias == null ? wrapField(getDbCategory(), columnName): wrapField(getDbCategory(), columnName) + " AS " + alias;
        }
    }
    
    public static class Table extends Clause{
        private String rawTableName;
        private String alias;
        private String tableShardId;
        private Object tableShardValue;
        
        public Table(String rawTableName) {
            this.rawTableName = rawTableName;
        }
        
        public Table(String rawTableName, String alias) {
            this(rawTableName);
            this.alias = alias;
        }
        
        public Table inShard(String tableShardId) {
            this.tableShardId = tableShardId;
            return this;
        }
        
        public Table shardValue(String tableShardValue) {
            this.tableShardValue = tableShardValue;
            return this;
        }
        
        public Table as(String alias) {
            this.alias = alias;
            return this;
        }
        
        @Override
        public String build() throws SQLException {
            String logicDbName = getLogicDbName();
            DatabaseCategory dbCategory = getDbCategory();
            String tableName = null;

            if(!isTableShardingEnabled(logicDbName, rawTableName))
                tableName = wrapField(dbCategory, rawTableName);
            else if(tableShardId!= null)
                tableName = wrapField(dbCategory, rawTableName + buildShardStr(logicDbName, tableShardId));
            else if(tableShardValue != null) {
                tableName = wrapField(dbCategory, rawTableName + buildShardStr(logicDbName, locateTableShardId(logicDbName, rawTableName, new DalHints().setTableShardValue(tableShardValue), null, null)));
            }else
                tableName = wrapField(dbCategory, rawTableName + buildShardStr(logicDbName, locateTableShardId(logicDbName, rawTableName, getHints(), getParameters(), null)));
            
            return alias == null ? tableName : tableName + " AS " + alias;
        }
    }
    
    /**
     * Special Clause that only works when DB is sql server. It will append WITH (NOLOCK) after table
     * name against guideline.
     * @author jhhe
     *
     */
    private static class SqlServerWithNoLock extends Text {
        private static final String SQL_SERVER_NOLOCK = "WITH (NOLOCK)";
        
        public SqlServerWithNoLock() {
            super(SQL_SERVER_NOLOCK);
        }

        public String build() {
            return getDbCategory() == DatabaseCategory.SqlServer ? super.build() : EMPTY;
        }
    }
}
