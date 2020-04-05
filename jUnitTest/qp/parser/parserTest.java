package qp.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.StringReader;

import org.junit.jupiter.api.Test;
import qp.utils.SQLQuery;

class parserTest {
    @Test
    public void selectStarOnly() throws Exception {
        SQLQuery query = buildQuery("SELECT * FROM AIRCRAFTS");
        assertEquals(1, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals(0, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertFalse(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectStarWhere() throws Exception {
        SQLQuery query = buildQuery("SELECT * FROM AIRCRAFTS WHERE AIRCRAFTS.cruisingrange  > \"100\"");
        assertEquals(1, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals(1, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertFalse(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectAttribute() throws Exception {
        SQLQuery query = buildQuery("SELECT AIRCRAFTS.aname FROM AIRCRAFTS");
        assertEquals(1, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals(0, query.getSelectionList().size());
        assertEquals(1, query.getProjectList().size());
        assertEquals("AIRCRAFTS.aname", query.getProjectList().get(0).toString());
        assertEquals(0, query.getNumJoin());
        assertFalse(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectAttributeWhere() throws Exception {
        SQLQuery query = buildQuery("SELECT AIRCRAFTS.aname FROM AIRCRAFTS WHERE AIRCRAFTS.aid = \"2\"");
        assertEquals(1, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals(1, query.getSelectionList().size());
        assertEquals(1, query.getProjectList().size());
        assertEquals("AIRCRAFTS.aname", query.getProjectList().get(0).toString());
        assertEquals(0, query.getNumJoin());
        assertFalse(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectMoreAttributesWhere() throws Exception {
        SQLQuery query = buildQuery("SELECT AIRCRAFTS.aname, AIRCRAFTS.cruisingrange FROM AIRCRAFTS WHERE AIRCRAFTS.aid = \"2\"");
        assertEquals(1, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals(1, query.getSelectionList().size());
        assertEquals(2, query.getProjectList().size());
        assertEquals("AIRCRAFTS.aname", query.getProjectList().get(0).toString());
        assertEquals("AIRCRAFTS.cruisingrange", query.getProjectList().get(1).toString());
        assertEquals(0, query.getNumJoin());
        assertFalse(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectStarJoin() throws Exception {
        SQLQuery query = buildQuery("SELECT * FROM AIRCRAFTS, EMPLOYEES");
        assertEquals(2, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals("EMPLOYEES", query.getFromList().get(1));
        assertEquals(0, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertFalse(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectStarJoinWhere() throws Exception {
        SQLQuery query = buildQuery("SELECT * FROM AIRCRAFTS, EMPLOYEES WHERE EMPLOYEES.salary = \"100\"");
        assertEquals(2, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals("EMPLOYEES", query.getFromList().get(1));
        assertEquals(1, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertFalse(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectDistinctStar() throws Exception {
        SQLQuery query = buildQuery("SELECT DISTINCT * FROM AIRCRAFTS");
        assertEquals(1, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals(0, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertTrue(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectDistinctStarWhere() throws Exception {
        SQLQuery query = buildQuery("SELECT DISTINCT * FROM AIRCRAFTS WHERE AIRCRAFTS.aid = \"11\"");
        assertEquals(1, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals(1, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertTrue(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectDistinctAttribute() throws Exception {
        SQLQuery query = buildQuery("SELECT DISTINCT AIRCRAFTS.aname FROM AIRCRAFTS");
        assertEquals(1, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals(0, query.getSelectionList().size());
        assertEquals(1, query.getProjectList().size());
        assertEquals("AIRCRAFTS.aname", query.getProjectList().get(0).toString());
        assertEquals(0, query.getNumJoin());
        assertTrue(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectDistinctAttributeWhere() throws Exception {
        SQLQuery query = buildQuery("SELECT DISTINCT AIRCRAFTS.aname FROM AIRCRAFTS WHERE AIRCRAFTS.aid = \"2\"");
        assertEquals(1, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals(1, query.getSelectionList().size());
        assertEquals(1, query.getProjectList().size());
        assertEquals("AIRCRAFTS.aname", query.getProjectList().get(0).toString());
        assertEquals(0, query.getNumJoin());
        assertTrue(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectDistinctMoreAttributesWhere() throws Exception {
        SQLQuery query = buildQuery("SELECT DISTINCT AIRCRAFTS.aname, AIRCRAFTS.cruisingrange FROM AIRCRAFTS WHERE AIRCRAFTS.aid = \"2\"");
        assertEquals(1, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals(1, query.getSelectionList().size());
        assertEquals(2, query.getProjectList().size());
        assertEquals("AIRCRAFTS.aname", query.getProjectList().get(0).toString());
        assertEquals("AIRCRAFTS.cruisingrange", query.getProjectList().get(1).toString());
        assertEquals(0, query.getNumJoin());
        assertTrue(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectDistinctStarJoin() throws Exception {
        SQLQuery query = buildQuery("SELECT DISTINCT * FROM AIRCRAFTS, EMPLOYEES");
        assertEquals(2, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals("EMPLOYEES", query.getFromList().get(1));
        assertEquals(0, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertTrue(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectDistinctStarJoinWhere() throws Exception {
        SQLQuery query = buildQuery("SELECT DISTINCT * FROM AIRCRAFTS, EMPLOYEES WHERE EMPLOYEES.salary = \"100\"");
        assertEquals(2, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals("EMPLOYEES", query.getFromList().get(1));
        assertEquals(1, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertTrue(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectStarLimit() throws Exception {
        SQLQuery query = buildQuery("SELECT * FROM AIRCRAFTS LIMIT \"8\"");
        assertEquals(1, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals(0, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertFalse(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(8,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectStarOffset() throws Exception {
        SQLQuery query = buildQuery("SELECT * FROM AIRCRAFTS OFFSET \"8\"");
        assertEquals(1, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals(0, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertFalse(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(8,query.getOffset());
    }

    @Test
    public void selectStarLimitOffset() throws Exception {
        SQLQuery query = buildQuery("SELECT * FROM AIRCRAFTS LIMIT \"8\" OFFSET \"3\"");
        assertEquals(1, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals(0, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertFalse(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(8,query.getLimit());
        assertEquals(3,query.getOffset());
    }

    @Test
    public void selectStarJoinWhereLimit() throws Exception {
        SQLQuery query = buildQuery("SELECT * FROM AIRCRAFTS, EMPLOYEES WHERE EMPLOYEES.salary = \"100\" LIMIT \"5\"");
        assertEquals(2, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals("EMPLOYEES", query.getFromList().get(1));
        assertEquals(1, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertFalse(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(5,query.getLimit());
        assertEquals(0,query.getOffset());
    }

    @Test
    public void selectStarJoinWhereOffset() throws Exception {
        SQLQuery query = buildQuery("SELECT * FROM AIRCRAFTS, EMPLOYEES WHERE EMPLOYEES.salary = \"100\" OFFSET \"3\"");
        assertEquals(2, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals("EMPLOYEES", query.getFromList().get(1));
        assertEquals(1, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertFalse(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(-1,query.getLimit());
        assertEquals(3,query.getOffset());
    }

    @Test
    public void selectStarJoinWhereLimitOffset() throws Exception {
        SQLQuery query = buildQuery("SELECT * FROM AIRCRAFTS, EMPLOYEES WHERE EMPLOYEES.salary = \"100\" LIMIT \"5\" OFFSET \"3\"");
        assertEquals(2, query.getFromList().size());
        assertEquals("AIRCRAFTS", query.getFromList().get(0));
        assertEquals("EMPLOYEES", query.getFromList().get(1));
        assertEquals(1, query.getSelectionList().size());
        assertEquals(0, query.getProjectList().size());
        assertEquals(0, query.getNumJoin());
        assertFalse(query.isDistinct());
        assertEquals(0,query.getGroupByList().size());
        assertEquals(0,query.getOrderByList().size());
        assertEquals(5,query.getLimit());
        assertEquals(3,query.getOffset());
    }

    private SQLQuery buildQuery(String input) throws Exception {
        StringReader query = new StringReader(input);
        Scaner scanner = new Scaner(query);
        parser p = new parser(scanner);
        p.parse();
        return p.getSQLQuery();
    }
}