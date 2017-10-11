package com.ctrip.platform.dal.dao.sqlbuilder;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Parent of AbstractFreeSqlBuilder and AbstractTableSqlBuilder
 * 
 * @author jhhe
 */
public abstract class AbstractSqlBuilder implements SqlBuilder {
    private static final String SPACE = " ";
    
    private BuilderContext context;
    private ClauseList clauses = new ClauseList();
    private boolean enableAutoMeltdown = true;
    private boolean enableSmartSpaceSkipping= true;

    public AbstractSqlBuilder(BuilderContext context) {
        clauses.setContext(context);
        this.context = context;
    }
    
    public BuilderContext getContext() {
        return context;
    }

    /**
     * Default logic for building the sql statement.
     * 
     * It will append where and check if the value is start of "and" or "or", of so, the leading 
     * "and" or "or" will be removed.
     */
    @Override
    public String build() {
        try {
            List<Clause> clauseList = clauses.getList();

            if(enableAutoMeltdown)
                clauseList = (List<Clause>)meltdown(clauseList);
            
            return concat(clauseList);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Disable the auto removal of AND, OR, NOT, (, ) and nullable expression
     */
    public void disableAutoMeltdown() {
        enableAutoMeltdown = false;
    }
    
    /**
     * Disable the auto space removing around bracket and before COMMA
     */
    public AbstractSqlBuilder disableSpaceSkipping() {
        enableSmartSpaceSkipping = false;
        return this;
    }
    
    /**
     * Enable the auto space removing around bracket and before COMMA
     */
    public AbstractSqlBuilder enableSpaceSkipping() {
        enableSmartSpaceSkipping = true;
        return this;
    }
    
    public void add(Clause clause) {
        clauses.add(clause);
    }
    
    public ClauseList getClauseList() {
        return clauses;
    }
    
    /**
     * If there is COMMA, then the leading space will not be appended.
     * If there is bracket, then both leading and trailing space will be omitted.
     * 
     * @param clauseList
     * @return
     * @throws SQLException
     */
    private String concat(List<Clause> clauseList) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < clauseList.size(); i ++) {
            Clause curClause = clauseList.get(i);
            Clause nextClause = (i == clauseList.size() - 1) ? null: clauseList.get(i+1);
            
            sb.append(curClause.build());
            
            if(skipSpaceInsertion(curClause, nextClause))
                continue;
            
            sb.append(SPACE);
        }
        
        return sb.toString().trim();
    }
    
    private List<Clause> meltdown(List<Clause> clauseList) {
        LinkedList<Clause> filtered = new LinkedList<>();
        
        for(Clause entry: clauseList) {
            if(entry.isExpression() && entry.isNull()){
                meltDownNullValue(filtered);
                continue;
            }

            if(entry.isBracket() && !entry.isLeft()){
                if(meltDownRightBracket(filtered))
                    continue;
            }
            
            // AND/OR
            if(entry.isOperator() && !entry.isNot()) {
                if(meltDownAndOrOperator(filtered))
                    continue;
            }
            
            filtered.add(entry);
        }
        
        return filtered;
    }
    
    /**
     * Builder will not insert space if enableSmartSpaceSkipping is enabled and:
     * 1. current cuase is operator(AND, OR, NOT)
     * 2. current cuase is left bracket
     * 3. next clause is right bracket or COMMA
     */
    private boolean skipSpaceInsertion(Clause curClause, Clause nextClause) {
        if(!enableSmartSpaceSkipping)
            return false;
        
        if(curClause.isOperator())
            return false;
        // if after "("
        if(curClause.isBracket() && curClause.isLeft())
            return true;
        
        // reach the end
        if(nextClause == null)
            return true;

        if(nextClause.isBracket() && !nextClause.isLeft())
            return true;
        
        return nextClause.isComma();
    }
    
    private void meltDownNullValue(LinkedList<Clause> filtered) {
        if(filtered.isEmpty())
            return;

        while(!filtered.isEmpty()) {
            Clause entry = filtered.getLast();
            // Remove any leading AND/OR/NOT (NOT is both operator and clause)
            if(entry.isOperator()) {
                filtered.removeLast();
            }else
                break;
        }
    }

    private static boolean meltDownRightBracket(LinkedList<Clause> filtered) {
        int bracketCount = 1;
        while(!filtered.isEmpty()) {
            Clause entry = filtered.getLast();
            // One ")" only remove one "("
            if(entry.isBracket() && entry.isLeft() && bracketCount == 1){
                filtered.removeLast();
                bracketCount--;
            } else if(entry.isOperator()) {// Remove any leading AND/OR/NOT (NOT is both operator and clause)
                filtered.removeLast();
            } else
                break;
        }
        
        return bracketCount == 0? true : false;
    }

    private static boolean meltDownAndOrOperator(LinkedList<Clause> filtered) {
        // If it is the first element
        if(filtered.isEmpty())
            return true;

        Clause entry = filtered.getLast();

        // If it is not a removable clause. Reach the beginning of the meltdown section
        if(!entry.isExpression())
            return true;

        // The last one is "("
        if(entry.isBracket() && entry.isLeft())
            return true;
            
        // AND/OR/NOT AND/OR
        if(entry.isOperator()) {
            return true;
        }
        
        return false;
    }
}
