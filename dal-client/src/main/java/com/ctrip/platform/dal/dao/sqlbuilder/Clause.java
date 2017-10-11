package com.ctrip.platform.dal.dao.sqlbuilder;

import java.sql.SQLException;

import com.ctrip.platform.dal.common.enums.DatabaseCategory;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.StatementParameters;


/**
 * Base class for sql clause.
 * 
 * It classifies clause in the following aspects:
 * 
 * 1. if it is an expression and if the value is nyull
 * 2. if it is a bracket and if the bracket is lfet one
 * 3. if it is an operator and if the operator is NOT
 * 4. if it is a comma
 * 
 * The classification will help to do clause meltdown or space insertion.
 * 
 * Because certain information may not be ready during clause append,
 * the build process is separated into two phases. One is preparing: setBuilderCOntext(), this 
 * is invoked immediately after clause is been constructed and added. The other is build(), which 
 * actually build part of the final sql.
 * 
 * @author jhhe
 *
 */
public abstract class Clause {
    private BuilderContext context;
    
    /**
     * @return the final sql segment
     * @throws SQLException
     */
    public abstract String build() throws SQLException;
    
    public void setContext(BuilderContext context) {
        this.context = context;
    }
    
    public DatabaseCategory getDbCategory() {
        return context.getDbCategory();
    }

    public String getLogicDbName() {
        return context.getLogicDbName();
    }

    public DalHints getHints() {
        return context.getHints();
    }

    public StatementParameters getParameters() {
        return context.getParameters();
    }

    /**
     * @return if current clause is comma
     */
    public boolean isComma() {
        return false;
    }

    /**
     * @return if current clause is an expression
     */
    public boolean isExpression() {
        return false;
    }

    /**
     * @return if current clause is null
     */
    public boolean isNull() {
        return false;
    }
    
    /**
     * @return if current clause is a bracket
     */
    public boolean isBracket() {
        return false;
    }
    
    /**
     * @return if current clause is left bracket
     */
    public boolean isLeft() {
        return false;
    }
    
    /**
     * @return if current clause is an operator
     */
    public boolean isOperator() {
        return false;
    }
    
    /**
     * @return if current clause is NOT operator
     */
    public boolean isNot() {
        return false;
    }    
}
