package com.ctrip.platform.dal.dao.sqlbuilder;

import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractTableSqlBuilder.wrapField;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import com.ctrip.platform.dal.dao.StatementParameter;
import com.ctrip.platform.dal.dao.StatementParameters;

/**
 * A factory of static expression methods.
 * 
 * @author jhhe
 *
 */
public class Expressions {
    public static final NullExpression NULL = new NullExpression();
    
    public static final ImmutableExpression TRUE = new ImmutableExpression("TRUE");
    
    public static final ImmutableExpression FALSE = new ImmutableExpression("FALSE");
    
    public static final Operator AND = new Operator("AND");
    
    public static final Operator OR = new Operator("OR");
    
    public static final Operator NOT = new Operator("NOT");
    
    public static final Bracket leftBracket = new Bracket(true);

    public static final Bracket rightBracket = new Bracket(false);
    
    public static ColumnExpression columnExpression(String template, String columnName) {
        return new ColumnExpression(template, columnName);
    }
    
    /**
     * Create Expression clause with the given template
     * @param template
     * @return
     */
    public static Expression expression(String template) {
        return new Expression(template);
    }

    public static Expression expression(boolean condition, String template) {
        return condition ? new Expression(template) : NULL;
    }
    
    public static Expression expression(boolean condition, String template, String elseTemplate) {
        return condition ? expression(template) : expression(elseTemplate);
    }
    
    public static Clause bracket(Clause... clauses) {
        ClauseList list = new ClauseList();
        return list.add(leftBracket).add(clauses).add(rightBracket);
    }

    public static ColumnExpression equal(String columnName) {
        return columnExpression("%s = ?", columnName);
    }
    
    public static ColumnExpression equal(String columnName, int sqlType, Object value) {
        return equal(columnName).set(sqlType, value);
    }
    
    public static ColumnExpression notEqual(String columnName) {
        return columnExpression("%s <> ?", columnName);
    }
    
    public static ColumnExpression notEqual(String columnName, int sqlType, Object value) {
        return notEqual(columnName).set(sqlType, value);
    }
    
    public static ColumnExpression greaterThan(String columnName) {
        return columnExpression("%s > ?", columnName);
    }

    public static ColumnExpression greaterThan(String columnName, int sqlType, Object value) {
        return greaterThan(columnName).set(sqlType, value);
    }

    public static ColumnExpression greaterThanEquals(String columnName) {
        return columnExpression("%s >= ?", columnName);
    }

    public static ColumnExpression greaterThanEquals(String columnName, int sqlType, Object value) {
        return greaterThanEquals(columnName).set(sqlType, value);
    }

    public static ColumnExpression lessThan(String columnName) {
        return columnExpression("%s < ?", columnName);
    }

    public static ColumnExpression lessThan(String columnName, int sqlType, Object value) {
        return lessThan(columnName).set(sqlType, value);
    }

    public static ColumnExpression lessThanEquals(String columnName) {
        return columnExpression("%s <= ?", columnName);
    }

    public static ColumnExpression lessThanEquals(String columnName, int sqlType, Object value) {
        return lessThanEquals(columnName).set(sqlType, value);
    }

    public static BetweenExpression between(String columnName) {
        return new BetweenExpression(columnName);
    }
    
    public static BetweenExpression between(String columnName, int sqlType, Object lowerValue, Object upperValue) {
        BetweenExpression between = new BetweenExpression(columnName);
        between.set(sqlType, lowerValue);
        return between.setUpperValue(upperValue);
    }
    
    public static ColumnExpression like(String columnName) {
        return columnExpression("%s LIKE ?", columnName);
    }
    
    public static ColumnExpression like(String columnName, int sqlType, Object value) {
        return like(columnName).set(sqlType, value);
    }
    
    public static ColumnExpression notLike(String columnName) {
        return columnExpression("%s NOT LIKE ?", columnName);
    }
    
    public static ColumnExpression notLike(String columnName, int sqlType, Object value) {
        return notLike(columnName).set(sqlType, value);
    }
    
    public static ColumnExpression in(String columnName) {
        return new InExpression(columnName);
    }
    
    public static ColumnExpression in(String columnName, int sqlType, Collection<?> values) {
        return in(columnName).set(sqlType, values);
    }
    
    public static ColumnExpression notIn(String columnName) {
        return new NotInExpression(columnName);
    }
    
    public static ColumnExpression notIn(String columnName, int sqlType, Collection<?> values) {
        return notIn(columnName).set(sqlType, values);
    }
    
    public static ColumnExpression isNull(String columnName) {
        return columnExpression("%s IS NULL", columnName);
    }
    
    public static ColumnExpression isNotNull(String columnName) {
        return columnExpression("%s IS NOT NULL", columnName);
    }
    
    public static class Operator extends Clause {
        private String operator;
        public Operator(String operator) {
            this.operator = operator;
        }
        
        @Override
        public String build() {
            return operator;
        }
    }
    
    public static class Bracket extends Clause {
        private boolean left;
        public Bracket(boolean isLeft) {
            left = isLeft;
        }

        public String build() {
            return left? "(" : ")";
        }
        
        public boolean isLeft() {
            return left;
        }
    }
    
    public static class Expression extends Clause {
        protected String template;
        private boolean invalid = false;
        
        public Expression(String template) {
            this.template = template;
        }
        
        public Expression nullable(Object o) {
            when(o != null);
            return this;
        }
        
        public Expression when(Boolean condition) {
            invalid = !condition;
            return this;
        }
        
        
        public boolean isInvalid() {
            return invalid;
        }
        
        public boolean isValid() {
            return !invalid;
        }
        
        public String build() {
            if(invalid)
                throw new IllegalStateException("This expression is invalid and should be removed instead of build");
            
            return template;
        }
    }
    
    public static class ImmutableExpression extends Expression {
        public ImmutableExpression(String template) {
            super(template);
        }
        public Expression nullable(Object o) {
            return this;
        }
        
        public Expression when(Boolean condition) {
            return this;
        }
        
        
        public boolean isInvalid() {
            return false;
        }
    }
    
    public static class ColumnExpression extends Expression {
        protected String columnName;
        protected int sqlType;
        protected Object value;
        protected StatementParameter parameter;
        
        public ColumnExpression(String template, String columnName) {
            super(template);
            Objects.requireNonNull(columnName, "column name can not be null");
            this.columnName = columnName;
        }
        
        public String getColumnName() {
            return columnName;
        }

        public ColumnExpression set(int sqlType, Object value) {
            if(parameter != null)
                throw new IllegalStateException("An expression can not be set twice!");
            
            this.sqlType = sqlType;
            this.value = value;
            StatementParameters parameters = getParameters();
            parameter = new StatementParameter(parameters.nextIndex(), sqlType, value).setName(columnName);
            parameters.add(parameter);
            return this;
        }
        
        public ColumnExpression nullable() {
            nullable(value);
            return this;
        }
        
        public Expression when(Boolean condition) {
            super.when(condition);
            parameter.when(condition);
            return this;
        }

        public String build() {
            String template = super.build();
            return columnName == null ? template : String.format(template, wrapField(getDbCategory(), columnName));
        }
    }
    
    public static class BetweenExpression extends ColumnExpression {
        private Object upperValue;
        private StatementParameter upperParameter;
        
        public BetweenExpression(String columnName) {
            super("%s BETWEEN ? AND ?", columnName);
        }
        
        public ColumnExpression nullable() {
            when(value != null && upperValue != null);
            return this;
        }
        
        public BetweenExpression setUpperValue(Object upperValue) {
            if(upperParameter != null)
                throw new IllegalStateException("An expression can not be set twice!");

            StatementParameters parameters = getParameters();
            upperParameter = new StatementParameter(parameters.nextIndex(), sqlType, value).setName(columnName);
            parameters.add(upperParameter);
            
            this.upperValue = upperValue;
            return this;
        }
        
        public Expression when(Boolean condition) {
            super.when(condition);
            upperParameter.when(condition);
            return this;
        }
    }
    
    public static class InExpression extends ColumnExpression {
        public InExpression(String columnName) {
            super("%s IN ( ? )", columnName);
        }
        
        public ColumnExpression nullable() {
            when(StatementParameter.isNullInParams((List<?>)value));
            return this;
        }
        
        public ColumnExpression set(int sqlType, Object value) {
            super.set(sqlType, value);
            getParameters().getLast().setInParam(true);
            return this;
        }
    }
    
    public static class NotInExpression extends InExpression {
        public NotInExpression(String columnName) {
            super(columnName);
            template = "%s NOT IN ( ? )";
        }
    }
    
    /**
     * This clause is just a placeholder that can be removed from the expression clause list.
     * @author jhhe
     *
     */
    public static class NullExpression extends Expression {
        public NullExpression() {
            super("");
        }
        
        public boolean isInvalid() {
            return true;
        }
        
        @Override
        public String build() {
            return "";
        }
    }
}
