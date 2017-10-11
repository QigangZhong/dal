package test.com.ctrip.platform.dal.dao.sqlbuilder;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder;

public class AbstractFreeSqlBuilderMeltdownTest {
    private static final String logicDbName = "dao_test_sqlsvr_tableShard";
    private static final String tableName = "dal_client_test";

    @Test
    public void testEqual() throws SQLException {
        validate("equal", "[a] = ?");
        validate("equalNull", "");
        validate("equal AND equal", "[a] = ? AND [a] = ?");
        validate("equal AND equalNull", "[a] = ?");
        validate("equalNull AND equal", "[a] = ?");
        validate("equalNull AND equalNull", "");
        
        validate("( equal )", "([a] = ?)");
        validate("( equalNull )", "");
        validate("( equal AND equal )", "([a] = ? AND [a] = ?)");
        validate("( equal AND equalNull )", "([a] = ?)");
        validate("( equalNull AND equal )", "([a] = ?)");
        validate("( equalNull AND equalNull )", "");
    }
    
    @Test
    public void testLike() throws SQLException {
        validate("like", "[a] LIKE ?");
        validate("likeNull", "");
        validate("like AND like", "[a] LIKE ? AND [a] LIKE ?");
        validate("like AND likeNull", "[a] LIKE ?");
        validate("likeNull AND like", "[a] LIKE ?");
        validate("likeNull AND likeNull", "");
        
        validate("( like )", "([a] LIKE ?)");
        validate("( likeNull )", "");
        validate("( like AND like )", "([a] LIKE ? AND [a] LIKE ?)");
        validate("( like AND likeNull )", "([a] LIKE ?)");
        validate("( likeNull AND like )", "([a] LIKE ?)");
        validate("( likeNull AND likeNull )", "");
    }
    
    @Test
    public void testBetween() throws SQLException {
        validate("between", "[a] BETWEEN ? AND ?");
        validate("betweenNull", "");
        validate("between AND between", "[a] BETWEEN ? AND ? AND [a] BETWEEN ? AND ?");
        validate("between AND betweenNull", "[a] BETWEEN ? AND ?");
        validate("betweenNull AND between", "[a] BETWEEN ? AND ?");
        validate("betweenNull AND betweenNull", "");
        
        validate("( between )", "([a] BETWEEN ? AND ?)");
        validate("( betweenNull )", "");
        validate("( between AND between )", "([a] BETWEEN ? AND ? AND [a] BETWEEN ? AND ?)");
        validate("( between AND betweenNull )", "([a] BETWEEN ? AND ?)");
        validate("( betweenNull AND between )", "([a] BETWEEN ? AND ?)");
        validate("( betweenNull AND betweenNull )", "");
    }
    
    @Test
    public void testIsNull() throws SQLException {
        validate("isNull", "[a] IS NULL");
        validate("isNull AND isNull", "[a] IS NULL AND [a] IS NULL");
        
        validate("( isNull )", "([a] IS NULL)");
        validate("( isNull AND isNull )", "([a] IS NULL AND [a] IS NULL)");
    }
    
    @Test
    public void testIsNotNull() throws SQLException {
        validate("isNotNull", "[a] IS NOT NULL");
        validate("isNotNull AND isNotNull", "[a] IS NOT NULL AND [a] IS NOT NULL");
        
        validate("( isNotNull )", "([a] IS NOT NULL)");
        validate("( isNotNull AND isNotNull )", "([a] IS NOT NULL AND [a] IS NOT NULL)");
    }
    
    @Test
    public void testNot() throws SQLException {
        validate("NOT equal", "NOT [a] = ?");
        validate("NOT equalNull", "");
        validate("NOT NOT NOT equal", "NOT NOT NOT [a] = ?");
        validate("NOT NOT NOT equalNull", "");
        validate("NOT equal AND NOT equal", "NOT [a] = ? AND NOT [a] = ?");
        validate("NOT equal AND NOT equalNull", "NOT [a] = ?");
        validate("NOT equalNull AND NOT equal", "NOT [a] = ?");
        validate("NOT equalNull AND NOT equalNull", "");
        
        validate("( NOT equal )", "(NOT [a] = ?)");
        validate("( NOT NOT NOT equal )", "(NOT NOT NOT [a] = ?)");
        validate("( NOT equalNull )", "");
        validate("( NOT NOT NOT equalNull )", "");
        validate("( NOT equal AND NOT equal )", "(NOT [a] = ? AND NOT [a] = ?)");
        validate("( NOT equal AND NOT equalNull )", "(NOT [a] = ?)");
        validate("( NOT equalNull AND NOT equal )", "(NOT [a] = ?)");
        validate("( NOT equalNull AND NOT equalNull )", "");
    }
    
    @Test
    public void testBracket() throws SQLException {
        validate("( ( equalNull ) )", "");
        validate("( ( ( equalNull ) ) )", "");
        validate("( ( ( equalNull ) ) ) AND ( ( ( equalNull ) ) )", "");
        validate("( ( ( equalNull ) ) ) OR ( ( ( equalNull ) ) )", "");
        validate("NOT ( NOT ( NOT ( NOT equalNull ) ) ) OR ( ( ( equalNull ) ) )", "");
    }
    
    @Test
    public void testOr() throws SQLException {
        validate("equal OR equal", "[a] = ? OR [a] = ?");
        validate("equal AND ( equal OR equal )", "[a] = ? AND ([a] = ? OR [a] = ?)");
    }

    public void validate(String exp, String expected) throws SQLException {
        AbstractFreeSqlBuilder builder = new AbstractFreeSqlBuilder().setLogicDbName(logicDbName);
        
        // equal equalNull between betweenNull in inNull like likeNull isNull isNotNull AND OR NOT ( )
        String[] tokens = exp.split(" "); 
        for(String token: tokens) {
            switch (token) {
                case "equal":
                    builder.equal("a");
                    break;
                case "equalNull":
                    builder.equal("a").nullable(null);
                    break;
                case "like":
                    builder.like("a");
                    break;
                case "likeNull":
                    builder.like("a").nullable(null);
                    break;
                case "isNull":
                    builder.isNull("a");
                    break;
                case "isNotNull":
                    builder.isNotNull("a");
                    break;
                case "in":
                    List<?> l = new ArrayList<>();
                    builder.in("a");
                    break;
                case "between":
                    builder.between("a");
                    break;
                case "inNull":
                    builder.in("a").nullable(null);
                    break;
                case "betweenNull":
                    builder.between("a").nullable(null);
                    break;
                case "AND":
                    builder.and();
                    break;
                case "OR":
                    builder.or();
                    break;
                case "NOT":
                    builder.not();
                    break;
                case "(":
                    builder.leftBracket();
                    break;
                case ")":
                    builder.rightBracket();
                    break;
                default:
                    Assert.fail("Unknown token: " + token);
            }
        }
        
        Assert.assertEquals(expected, builder.build());
    }
}
