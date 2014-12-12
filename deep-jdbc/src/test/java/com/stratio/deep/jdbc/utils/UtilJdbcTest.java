package com.stratio.deep.jdbc.utils;

import static org.testng.Assert.*;

import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.jdbc.testentity.MessageTestEntity;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mariomgal on 12/12/14.
 */
@Test(groups = { "UnitTests" })
public class UtilJdbcTest {

    @Test
    public void testGetObjectFromRow() throws Exception {
        Map<String, Object> row = createRow();
        MessageTestEntity entity = UtilJdbc.getObjectFromRow(MessageTestEntity.class, row, new JdbcDeepJobConfig(MessageTestEntity.class));

        assertEquals(entity.getId(), row.get("id"));
        assertEquals(entity.getMessage(), row.get("message"));
        assertEquals(entity.getNumber(), row.get("number"));
    }

    @Test
    public void testGetRowFromObject() throws Exception {
        MessageTestEntity entity = createMessageTestEntity();
        Map<String, Object> row = UtilJdbc.getRowFromObject(entity);

        assertEquals(row.get("id"), entity.getId());
        assertEquals(row.get("message"), entity.getMessage());
        assertEquals(row.get("number"), entity.getNumber());
    }

    @Test
    public void testGetCellsFromObject() {
        Map<String, Object> row = createRow();
        Cells cells = UtilJdbc.getCellsFromObject(row, new JdbcDeepJobConfig(Cells.class));

        assertEquals(cells.getCellByName("id").getValue(), row.get("id"));
        assertEquals(cells.getCellByName("message").getValue(), row.get("message"));
        assertEquals(cells.getCellByName("number").getValue(), row.get("number"));
    }

    @Test
    public void testGetObjectFromCell() {
        Cells cells = createCells();
        Map<String, Object> row = UtilJdbc.getObjectFromCells(cells);

        assertEquals(row.get("id"), cells.getCellByName("id").getValue());
        assertEquals(row.get("message"), cells.getCellByName("message").getValue());
        assertEquals(row.get("number"), cells.getCellByName("number").getValue());
    }

    private Map<String, Object> createRow() {
        Map<String, Object> row = new HashMap<>();
        row.put("id", "id");
        row.put("message", "test message");
        row.put("number", 1L);
        return row;
    }

    private MessageTestEntity createMessageTestEntity() {
        MessageTestEntity entity = new MessageTestEntity();
        entity.setId("id");
        entity.setMessage("test message");
        entity.setNumber(1L);
        return entity;
    }

    private Cells createCells() {
        Cells cells = new Cells();
        cells.add(Cell.create("id", "id"));
        cells.add(Cell.create("message", "test message"));
        cells.add(Cell.create("number", 1L));
        return cells;
    }
}
