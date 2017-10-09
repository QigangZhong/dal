package com.ctrip.platform.dal.dao.sqlbuilder;


/**
 * This interface classifies clause in the following three aspects:
 * 
 * 1. if it is an expression and if the value is nyull
 * 2. if it is a bracket and if the bracket is lfet one
 * 3. if it is an operator and if the operator is NOT
 * 
 * The classification will help MeltdownHelper to do cuase meltdown 
 * when there is nullable expression
 * 
 * @author jhhe
 *
 */
public interface ClauseClassifier {
    /**
     * @return if current clause is an expression
     */
    boolean isExpression();

    /**
     * @return if current clause is null
     */
    boolean isNull();
    
    /**
     * @return if current clause is a bracket
     */
    boolean isBracket();
    
    /**
     * @return if current clause is left bracket
     */
    boolean isLeft();
    
    
    /**
     * @return if current clause is an operator
     */
    boolean isOperator();
    
    /**
     * @return if current clause is NOT operator
     */
    boolean isNot();
}
