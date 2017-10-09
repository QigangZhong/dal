package com.ctrip.platform.dal.dao.sqlbuilder;

import java.util.LinkedList;
import java.util.List;

public class MeltdownHelper {

    public static List<? extends ClauseClassifier> meltdown(List<? extends ClauseClassifier> clauseList) {
        LinkedList<ClauseClassifier> filtered = new LinkedList<>();
        
        for(ClauseClassifier entry: clauseList) {
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
    
    private static void meltDownNullValue(LinkedList<ClauseClassifier> filtered) {
        if(filtered.isEmpty())
            return;

        while(!filtered.isEmpty()) {
            ClauseClassifier entry = filtered.getLast();
            // Remove any leading AND/OR/NOT (NOT is both operator and clause)
            if(entry.isOperator()) {
                filtered.removeLast();
            }else
                break;
        }
    }

    private static boolean meltDownRightBracket(LinkedList<ClauseClassifier> filtered) {
        int bracketCount = 1;
        while(!filtered.isEmpty()) {
            ClauseClassifier entry = filtered.getLast();
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

    private static boolean meltDownAndOrOperator(LinkedList<ClauseClassifier> filtered) {
        // If it is the first element
        if(filtered.isEmpty())
            return true;

        ClauseClassifier entry = filtered.getLast();

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
