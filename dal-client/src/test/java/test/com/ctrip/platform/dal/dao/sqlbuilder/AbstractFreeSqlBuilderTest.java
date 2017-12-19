package test.com.ctrip.platform.dal.dao.sqlbuilder;

import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.column;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.table;
import static com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.text;
import static com.ctrip.platform.dal.dao.sqlbuilder.Expressions.AND;
import static com.ctrip.platform.dal.dao.sqlbuilder.Expressions.OR;
import static com.ctrip.platform.dal.dao.sqlbuilder.Expressions.expression;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.StatementParameters;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder;
import com.ctrip.platform.dal.dao.sqlbuilder.AbstractFreeSqlBuilder.Text;
import com.ctrip.platform.dal.dao.sqlbuilder.Clause;
import com.ctrip.platform.dal.dao.sqlbuilder.Expressions.Expression;

public class AbstractFreeSqlBuilderTest {
    private static final String template = "template";
    private static final String wrappedTemplate = "[template]";
    private static final String expression = "count()";
    private static final String elseTemplate = "elseTemplate";
    private static final String EMPTY = "";
    private static final String logicDbName = "dao_test_sqlsvr_tableShard";
    private static final String tableName = "dal_client_test";
    
    @Test
    public void testSetLogicDbName() {
        AbstractFreeSqlBuilder test = create();
        try {
            test.setLogicDbName(null);
            fail();
        } catch (Exception e) {
        }
        
        try {
            test.setLogicDbName("Not exist");
            fail();
        } catch (IllegalArgumentException e) {
        } catch(Throwable ex) {
            fail();
        }
        
        test.setLogicDbName(logicDbName);
    }
    
    /**
     * Create test with auto meltdown disabled
     * @return
     */
    private AbstractFreeSqlBuilder create() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        StatementParameters p = new StatementParameters();
        test.with(p);
        return test;
    }
    
    @Test
    public void testSetHints() {
        AbstractFreeSqlBuilder test = create();
        try {
            test.setHints(null);
            fail();
        } catch (Exception e) {
        }
        
        test.setHints(new DalHints());
    }
    
    @Test
    public void testWith() {
        AbstractFreeSqlBuilder test = create();
        try {
            test.with(null);
            fail();
        } catch (Exception e) {
        }
        
        StatementParameters p = new StatementParameters();
        test.with(p);
        // Same is allowed
        test.with(p);

        //Empty is allowed
        p = new StatementParameters();
        test.with(p);
        p.set("", 1, "");
        
        p = new StatementParameters();
        try {
            test.with(p);
            fail();
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testAppend() {
        AbstractFreeSqlBuilder test = create();
        test.append(template);
        assertEquals(template, test.build());
    }

    @Test
    public void testAppendCondition() {
        AbstractFreeSqlBuilder test = create();
        test.appendWhen(true, template);
        assertEquals(template, test.build());
        
        test = create();
        test.appendWhen(false, template);
        assertEquals(EMPTY, test.build());
    }
    
    @Test
    public void testAppendConditionWithElse() {
        AbstractFreeSqlBuilder test = create();
        test.appendWhen(true, template, elseTemplate);
        assertEquals(template, test.build());
        
        test = create();
        test.appendWhen(false, template, elseTemplate);
        assertEquals(elseTemplate, test.build());
    }
    
    @Test
    public void testAppendClause() {
        AbstractFreeSqlBuilder test = create();
        test.append(new Text(template));
        assertEquals(template, test.build());
    }

    @Test
    public void testAppendClauseCondition() {
        AbstractFreeSqlBuilder test = create();
        test.appendWhen(true, new Text(template));
        assertEquals(template, test.build());
        
        test = create();
        test.appendWhen(false, new Text(template));
        assertEquals(EMPTY, test.build());
    }
    
    @Test
    public void testAppendClauseConditionWithElse() {
        AbstractFreeSqlBuilder test = create();
        test.appendWhen(true, new Text(template), new Text(elseTemplate));
        assertEquals(template, test.build());
        
        test = create();
        test.appendWhen(false, new Text(template), new Text(elseTemplate));
        assertEquals(elseTemplate, test.build());
    }
    
    @Test
    public void testAppendColumn() {
        AbstractFreeSqlBuilder test = create();
        test.appendColumn(template);
        test.setLogicDbName(logicDbName);
        assertEquals("[" + template + "]", test.build());
        
        test = create();
        test.appendColumn(template, template);
        test.setLogicDbName(logicDbName);
        assertEquals("[" + template + "] AS " + template, test.build());

        test = create();
        test.append(column(template).as(template));
        test.setLogicDbName(logicDbName);
        assertEquals("[" + template + "] AS " + template, test.build());
    }
    
    @Test
    public void testAppendTable() {
        String noShardTable = "noShard";
        
        AbstractFreeSqlBuilder test = create();
        test.appendTable(noShardTable);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("[" + noShardTable + "]", test.build());
        
        test = create();
        test.appendTable(tableName);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints().inTableShard(1));
        assertEquals("[" + tableName + "_1]", test.build());
        
        test = create();
        test.appendTable(tableName, template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints().inTableShard(1));
        assertEquals("[" + tableName + "_1] AS " + template, test.build());

        test = create();
        test.append(table(tableName).as(template));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints().inTableShard(1));
        assertEquals("[" + tableName + "_1] AS " + template, test.build());
    }
    
    @Test
    public void testSelect() {
        String noShardTable = "noShard";
        
        AbstractFreeSqlBuilder test = create();
        test.select(template, template, template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("SELECT [template], [template], [template]", test.build());
        
        test = create();
        test.select(template, text(template), expression(template), column(template).as(template));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("SELECT [template], template, template, [template] AS template", test.build());
    }
    
    @Test
    public void testFrom() {
        String noShardTable = "noShard";
        
        AbstractFreeSqlBuilder test = create();
        test.from(noShardTable);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("FROM [noShard] WITH (NOLOCK)", test.build());
        
        test = create();
        test.from(table(noShardTable));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("FROM [noShard] WITH (NOLOCK)", test.build());
    }
    
    @Test
    public void testWhere() {
        AbstractFreeSqlBuilder test = create();
        test.where(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("WHERE template", test.build());
    }
    
    @Test
    public void testWhereClause() {
        AbstractFreeSqlBuilder test = create();
        test.where(expression("count()"), text(template));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("WHERE count() template", test.build());
    }
    
    @Test
    public void testOrderBy() {
        AbstractFreeSqlBuilder test = create();
        test.orderBy(template, true);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("ORDER BY " + wrappedTemplate + " ASC", test.build());
    }
    
    @Test
    public void testGroupBy() {
        AbstractFreeSqlBuilder test = create();
        test.groupBy(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("GROUP BY " + wrappedTemplate, test.build());
        
        test = create();
        test.groupBy(expression(template));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("GROUP BY " + template, test.build());
    }
    
    @Test
    public void testHaving() {
        AbstractFreeSqlBuilder test = create();
        test.having(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("HAVING " + template, test.build());
    }
    
    @Test
    public void testLeftBracket() {
        AbstractFreeSqlBuilder test = create();
        test.leftBracket();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("(", test.build());
    }
    
    @Test
    public void testRightBracket() {
        AbstractFreeSqlBuilder test = create();
        test.rightBracket();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(")", test.build());
    }
    
    @Test
    public void testBracket() {
        //Empty
        AbstractFreeSqlBuilder test = create();
        test.bracket();
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("", test.build());
        
        //One
        test = create();
        test.bracket(text(template));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("(template)", test.build());
        
        //two
        test = create();
        test.bracket(text(template), expression(expression));
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals("(template " + expression + ")", test.build());
    }
    
    @Test
    public void testAnd() {
        AbstractFreeSqlBuilder test = create();
        test.and();
        assertEquals("", test.build());

        test = create();
        test.and(template, template, template);
        assertEquals("template AND template AND template", test.build());
    }
    
    @Test
    public void testOr() {
        AbstractFreeSqlBuilder test = create();
        test.or();
        assertEquals("", test.build());

        test = create();
        test.or(template, expression);
        assertEquals(template + " OR " + expression, test.build());
    }
    
    @Test
    public void testNot() {
        AbstractFreeSqlBuilder test = create();
        test.not();
        assertEquals("NOT", test.build());
    }
    
    @Test
    public void testNullable() {
        AbstractFreeSqlBuilder test = create();
        Expression exp;
        try {
            test.nullable(null);
            fail();
        } catch (Exception e) {
        }
        
        test = create();
        try {
            test.append(template).nullable(null);
            fail();
        } catch (Exception e) {
        }

        test = create();
        try {
            test.append(template).nullable(new Object());
            fail();
        } catch (Exception e) {
        }

        test = create();
        exp = new Expression(expression);
        test.append(template).append(exp).nullable(null);
        assertTrue(exp.isInvalid());
        assertEquals(template, test.build());

        test = create();
        exp = new Expression(expression);
        test.append(template).append(exp).nullable(new Object());
        assertTrue(exp.isValid());
        assertEquals(template + " " + expression, test.build());        
    }
    
    @Test
    public void testWhen() {
        AbstractFreeSqlBuilder test = create();
        Expression exp;
        try {
            test.when(false);
            fail();
        } catch (Exception e) {
        }
        
        test = create();
        try {
            test.append(template).when(false);
            fail();
        } catch (Exception e) {
        }

        test = create();
        try {
            test.append(template).when(true);
            fail();
        } catch (Exception e) {
        }

        test = create();
        exp = new Expression(expression);
        test.append(template).append(exp).when(false);
        assertTrue(exp.isInvalid());
        assertEquals(template, test.build());
        
        test = create();
        exp = new Expression(expression);
        test.append(template).append(exp).when(true);
        assertTrue(exp.isValid());
        assertEquals(template + " " + expression, test.build());
    }
    
    private interface ExpressionProvider {
        AbstractFreeSqlBuilder createExp();
        AbstractFreeSqlBuilder createExpWithParameter();
        AbstractFreeSqlBuilder createExpWithNullParameter();
    }
    
    public void testExpression(String result, ExpressionProvider provider) {
        testExpression(result, 1, provider);
    }
    
    public void testExpression(String result, int count, ExpressionProvider provider) {
        testExpr(result, 0, provider.createExp());
        testExpr(result, count, provider.createExpWithParameter());
        
        testExpr(result, 0, provider.createExp().nullable(new Object()));        
        testExpr(result, count, provider.createExpWithParameter().nullable(new Object()));        

        testExpr(result, 0, provider.createExp().when(true));
        testExpr(result, count, provider.createExpWithParameter().when(true));
        
        testNullError(provider.createExp());
        testNull(provider.createExpWithNullParameter().nullable());
        testExpr(result, count, provider.createExpWithParameter().nullable());
        
        
        testNull(provider.createExp().nullable(null));
        testNull(provider.createExpWithParameter().nullable(null));
        
        testNull(provider.createExp().when(false));
        testNull(provider.createExpWithParameter().when(false));
    }
    
    @Test
    public void testEqual() {
        testExpression(wrappedTemplate + " = ?", new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().equal(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().equal(template, Types.VARCHAR, "abc");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().equal(template, Types.VARCHAR, null);
            }
        });
    }
    
    private void testExpr(String result, int count, AbstractFreeSqlBuilder builder) {
        assertEquals(result, builder.build());
        assertEquals(count, builder.buildParameters().size());
    }
    
    private void testNull(AbstractFreeSqlBuilder builder) {
        testExpr("", 0, builder);
    }
    
    private void testNullError(AbstractFreeSqlBuilder builder) {
        try {
            builder.nullable();
            fail();
        } catch (Throwable e) {
        }
    }
    
    @Test
    public void testNotEqual() {
        testExpression(wrappedTemplate + " <> ?", new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().notEqual(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().notEqual(template, Types.VARCHAR, "abc");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().notEqual(template, Types.VARCHAR, null);
            }
        });
    }
    
    @Test
    public void testGreaterThan() {
        testExpression(wrappedTemplate + " > ?", new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().greaterThan(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().greaterThan(template, Types.VARCHAR, "abc");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().greaterThan(template, Types.VARCHAR, null);
            }
        });
    }
    
    @Test
    public void testGreaterThanEquals() {
        testExpression(wrappedTemplate + " >= ?", new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().greaterThanEquals(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().greaterThanEquals(template, Types.VARCHAR, "abc");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().greaterThanEquals(template, Types.VARCHAR, null);
            }
        });
    }
    
    @Test
    public void testLessThan() {
        testExpression(wrappedTemplate + " < ?", new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().lessThan(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().lessThan(template, Types.VARCHAR, "abc");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().lessThan(template, Types.VARCHAR, null);
            }
        });
    }
    
    @Test
    public void testLessThanEquals() {
        testExpression(wrappedTemplate + " <= ?", new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().lessThanEquals(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().lessThanEquals(template, Types.VARCHAR, "abc");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().lessThanEquals(template, Types.VARCHAR, null);
            }
        });
    }
    
    @Test
    public void testLike() {
        testExpression(wrappedTemplate + " LIKE ?", new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().like(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().like(template, Types.VARCHAR, "abc");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().like(template, Types.VARCHAR, null);
            }
        });
    }
    
    @Test
    public void testNotLike() {
        testExpression(wrappedTemplate + " NOT LIKE ?", new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().notLike(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().notLike(template, Types.VARCHAR, "abc");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().notLike(template, Types.VARCHAR, null);
            }
        });
    }
    
    @Test
    public void testBetween() {
        String result = wrappedTemplate + " BETWEEN ? AND ?";
        testExpression(result, 2, new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().between(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().between(template, Types.VARCHAR, "abc", "def");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().between(template, Types.VARCHAR, null, null);
            }
        });
        
        testExpression(result, 2, new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().between(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().between(template, Types.VARCHAR, "abc", "def");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().between(template, Types.VARCHAR, "abc", null);
            }
        });

        testExpression(result, 2, new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().between(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().between(template, Types.VARCHAR, "abc", "def");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().between(template, Types.VARCHAR, null, "abc");
            }
        });
    }
    
    @Test
    public void testNotBetween() {
        String result = wrappedTemplate + " NOT BETWEEN ? AND ?";
        testExpression(result, 2, new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().notBetween(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().notBetween(template, Types.VARCHAR, "abc", "def");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().notBetween(template, Types.VARCHAR, null, null);
            }
        });
        
        testExpression(result, 2, new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().notBetween(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().notBetween(template, Types.VARCHAR, "abc", "def");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().notBetween(template, Types.VARCHAR, "abc", null);
            }
        });

        testExpression(result, 2, new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().notBetween(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                return create().notBetween(template, Types.VARCHAR, "abc", "def");
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().notBetween(template, Types.VARCHAR, null, "abc");
            }
        });
    }
    
    @Test
    public void testIn() {
        String result = wrappedTemplate + " IN ( ? )";
        testExpression(result, 1, new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().in(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                List l = new ArrayList<>();
                l.add("abc");
                return create().in(template, Types.VARCHAR, l);
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().in(template, Types.VARCHAR, null);
            }
        });

        testExpression(result, 1, new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().in(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                List l = new ArrayList<>();
                l.add("abc");
                return create().in(template, Types.VARCHAR, l);
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().in(template, Types.VARCHAR, new ArrayList<>());
            }
        });
        
        testExpression(result, 1, new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().in(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                List l = new ArrayList<>();
                l.add("abc");
                return create().in(template, Types.VARCHAR, l);
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                List l = new ArrayList<>();
                l.add(null);
                return create().in(template, Types.VARCHAR, l);
            }
        });
    }
    
    @Test
    public void testNotIn() {
        String result = wrappedTemplate + " NOT IN ( ? )";
        testExpression(result, 1, new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().notIn(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                List l = new ArrayList<>();
                l.add("abc");
                return create().notIn(template, Types.VARCHAR, l);
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().notIn(template, Types.VARCHAR, null);
            }
        });

        testExpression(result, 1, new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().notIn(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                List l = new ArrayList<>();
                l.add("abc");
                return create().notIn(template, Types.VARCHAR, l);
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                return create().notIn(template, Types.VARCHAR, new ArrayList<>());
            }
        });
        
        testExpression(result, 1, new ExpressionProvider() {
            public AbstractFreeSqlBuilder createExp() {
                return create().notIn(template);
            }
            public AbstractFreeSqlBuilder createExpWithParameter() {
                List l = new ArrayList<>();
                l.add("abc");
                return create().notIn(template, Types.VARCHAR, l);
            }
            public AbstractFreeSqlBuilder createExpWithNullParameter() {
                List l = new ArrayList<>();
                l.add(null);
                return create().notIn(template, Types.VARCHAR, l);
            }
        });
    }
    
    @Test
    public void testIsNull() {
        AbstractFreeSqlBuilder test = create();
        test.isNull(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " IS NULL", test.build());
        assertEquals(0, test.buildParameters().size());
    }
    
    @Test
    public void testIsNotNull() {
        AbstractFreeSqlBuilder test = create();
        test.isNotNull(template);
        test.setLogicDbName(logicDbName);
        test.setHints(new DalHints());
        assertEquals(wrappedTemplate + " IS NOT NULL", test.build());
        assertEquals(0, test.buildParameters().size());
    }
    
    @Test
    public void testExpression() throws SQLException {
        Clause test = expression(template);
        
        AbstractFreeSqlBuilder builder = create();
        builder.append(test);
        builder.setLogicDbName(logicDbName);

        assertEquals(template, test.build());
    }
    
    @Test
    public void testAutoMeltdown() throws SQLException {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.appendExpressions(AND).bracket(AND, OR, AND);
        assertEquals("", test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.appendExpressions(template, AND).bracket(AND, OR, AND);
        assertEquals(template, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template, AND).bracket(AND, OR, AND).appendColumn(template);
        assertEquals(template + " " +wrappedTemplate, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template, AND).bracket(AND, OR, AND).appendTable(template);
        assertEquals(template + " " + wrappedTemplate, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template).nullable(null).append(AND).bracket(AND, OR, AND).appendTable(template);
        assertEquals(wrappedTemplate, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template, AND).bracket(AND, OR, AND).appendTable(template).append(AND).append(expression(template)).nullable(null);
        assertEquals(template+ " " + wrappedTemplate, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template, AND).bracket(AND, OR, AND, template).appendTable(template).append(AND).append(expression(template)).nullable(null);
        assertEquals("template AND (template) [template]", test.build());
    }

    @Test
    public void testAutoMeltdownWhen() throws SQLException {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.appendExpressions(AND).bracket(AND, OR, AND);
        assertEquals("", test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.appendExpressions(template, AND).bracket(AND, OR, AND);
        assertEquals(template, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template, AND).bracket(AND, OR, AND).appendColumn(template);
        assertEquals(template + " " +wrappedTemplate, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template, AND).bracket(AND, OR, AND).appendTable(template);
        assertEquals(template + " " + wrappedTemplate, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template).when(false).append(AND).bracket(AND, OR, AND).appendTable(template);
        assertEquals(wrappedTemplate, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template, AND).bracket(AND, OR, AND).appendTable(template).append(AND).append(expression(template)).when(Boolean.FALSE == null);
        assertEquals(template+ " " + wrappedTemplate, test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.appendExpressions(template, AND).bracket(AND, OR, AND, template).appendTable(template).append(AND).append(expression(template)).when(Boolean.FALSE == null);
        assertEquals("template AND (template) [template]", test.build());
    }
    
    @Test
    public void testIncludeAll() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.select(template).from(template).where(AbstractFreeSqlBuilder.includeAll()).equal(template);
        assertEquals("SELECT [template] FROM [template] WITH (NOLOCK) WHERE TRUE AND [template] = ?", test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.select(template).from(template).where(AbstractFreeSqlBuilder.includeAll()).equal(template).nullable(null);
        assertEquals("SELECT [template] FROM [template] WITH (NOLOCK) WHERE TRUE", test.build());
    }
    
    @Test
    public void testExcludeAll() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.select(template).from(template).where(AbstractFreeSqlBuilder.excludeAll()).equal(template);
        assertEquals("SELECT [template] FROM [template] WITH (NOLOCK) WHERE FALSE OR [template] = ?", test.build());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        test.select(template).from(template).where(AbstractFreeSqlBuilder.excludeAll()).equal(template).nullable(null);
        assertEquals("SELECT [template] FROM [template] WITH (NOLOCK) WHERE FALSE", test.build());
    }    
    
    @Test
    public void testSet() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        StatementParameters p = new StatementParameters();
        test.with(p);
        test.select(template).from(template).where().equal(template).set(template, Types.VARCHAR, "abc");
        assertEquals("SELECT [template] FROM [template] WITH (NOLOCK) WHERE [template] = ?", test.build());
        StatementParameters parameters = test.buildParameters();
        assertEquals(1, parameters.size());
        assertEquals(template, parameters.get(0).getName());
    }
    
    @Test
    public void testSetNullable() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        StatementParameters p = new StatementParameters();
        test.with(p);
        test.select(template).from(template).where().equal(template).setNullable("abc", Types.VARCHAR, null).nullable(null);
        assertEquals("SELECT [template] FROM [template] WITH (NOLOCK) WHERE", test.build());
        StatementParameters parameters = test.buildParameters();
        assertEquals(0, parameters.size());
        
        test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        p = new StatementParameters();
        test.with(p);
        test.select(template).from(template).where().equal(template).setNullable("abc", Types.VARCHAR, null);
        assertEquals("SELECT [template] FROM [template] WITH (NOLOCK) WHERE [template] = ?", test.build());
        parameters = test.buildParameters();
        assertEquals(0, parameters.size());
    }
    
    @Test
    public void testSetWhen() {
        AbstractFreeSqlBuilder test = new AbstractFreeSqlBuilder();
        test.setLogicDbName(logicDbName);
        StatementParameters p = new StatementParameters();
        test.with(p);
        test.select(template).from(template).where().equal(template).set(false, template, Types.VARCHAR, "abc").when(false);
        assertEquals("SELECT [template] FROM [template] WITH (NOLOCK) WHERE", test.build());
        StatementParameters parameters = test.buildParameters();
        assertEquals(0, parameters.size());
    }
    
    @Test
    public void testSetIn() {
        
    }
}
