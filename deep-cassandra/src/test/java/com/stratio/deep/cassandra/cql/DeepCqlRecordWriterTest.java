package com.stratio.deep.cassandra.cql;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.json.simple.JSONValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.stratio.deep.cassandra.config.CassandraDeepJobConfig;
import com.stratio.deep.cassandra.querybuilder.DefaultQueryBuilder;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.utils.CellsUtils;
import com.stratio.deep.commons.utils.Pair;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ CassandraClientProvider.class })
public class DeepCqlRecordWriterTest {

    private static final String LOCALHOST = "localhost";

    private static final String CATALOG_NAME = "testKeyspace";

    private static final String TABLE_NAME = "testTable";

    private static final int DATA_SIZE = 6;
    private static final int MAX_BATCH_SIZE = DATA_SIZE + 2;

    private Random rand = new Random();

    @Mock
    private CassandraDeepJobConfig config;

    @Mock
    private Session session;

    @Test
    public void testNumWritesBasedOnBatchSize()
            throws IllegalAccessException, InstantiationException, InvocationTargetException {
        // Static stubs
        PowerMockito.mockStatic(CassandraClientProvider.class);

        // Stubbing
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        int batchSize = rand.nextInt(MAX_BATCH_SIZE) + 1;

        when(config.getBatchSize()).thenReturn(batchSize);
        when(CassandraClientProvider.trySessionForLocation(anyString(), any(CassandraDeepJobConfig.class),
                anyBoolean())).thenReturn(Pair.create(session, LOCALHOST));
        when(session.prepare(anyString())).thenReturn(preparedStatement);

        DefaultQueryBuilder queryBuilder = new DefaultQueryBuilder();
        queryBuilder.setCatalogName(CATALOG_NAME);
        queryBuilder.setTableName(TABLE_NAME);

        DeepCqlRecordWriter writer = new DeepCqlRecordWriter(config, queryBuilder);

        for (Cells cells: prepareData()) {
            Cell key = cells.getCellByName("id");
            List<Cell> values = Lists.newArrayList(Iterables.filter(cells, new Predicate<Cell>() {
                @Override
                public boolean apply(Cell cell) {
                    return !"id".equals(cell.getCellName());
                }
            }));

            writer.write(new Cells(key), new Cells(values.toArray(new Cell[values.size()])));
        }

        writer.close();

        int roundUpBatchExecutions = (int) Math.ceil((double)DATA_SIZE/batchSize);
        verify(session, Mockito.times(roundUpBatchExecutions)).execute(any(Statement.class));

    }

    private static List<Cells> prepareData() throws IllegalAccessException, InvocationTargetException,
            InstantiationException {
        List<Cells> teams = new ArrayList<>(6);
        String teamJson;
        Cells team;

        teamJson = "{\"id\":1, \"name\":\"FC Bayern München\", \"short_name\":\"FCB\", \"arena_name\":\"Allianz"
                + " Arena\", \"coach_name\":\"Josep Guardiola\", \"city_name\":\"München\", "
                + "\"league_name\":\"Bundesliga\"}";
        team = CellsUtils.getCellFromJson((org.json.simple.JSONObject) JSONValue.parse(teamJson), "testTable");
        teams.add(team);

        teamJson = "{\"id\":2, \"name\":\"Hamburger SV\", \"short_name\":\"HSV\", \"arena_name\":\"Imtech Arena\", "
                + "\"coach_name\":\"Josef Zinnbauer\", \"city_name\":\"Hamburg\", \"league_name\":\"Bundesliga\"}";
        team = CellsUtils.getCellFromJson((org.json.simple.JSONObject) JSONValue.parse(teamJson), "testTable");
        teams.add(team);

        teamJson = "{\"id\":3, \"name\":\"Herta BSC Berlin\", \"short_name\":\"Herta\", \"arena_name\":\"Olympiastaion Berlin\","
                + " \"coach_name\":\"Jos Luhukay\", \"city_name\":\"Berlin\", \"league_name\":\"Bundesliga\"}";
        team = CellsUtils.getCellFromJson((org.json.simple.JSONObject) JSONValue.parse(teamJson), "testTable");
        teams.add(team);

        teamJson = "{\"id\":4, \"name\":\"FC Basel 1893\", \"short_name\":\"FCB\", \"arena_name\":\"St. Jakob-Park\", "
                + "\"coach_name\":\"Paulo Sousa\", \"city_name\":\"Basel\", \"league_name\":\"Raiffeisen Super League\"}";
        team = CellsUtils.getCellFromJson((org.json.simple.JSONObject) JSONValue.parse(teamJson), "testTable");
        teams.add(team);

        teamJson = "{\"id\":5, \"name\":\"FC Paris Saint-Germain\", \"short_name\":\"PSG\", \"arena_name\":\"Parc des Princes\","
                + " \"coach_name\":\"Laurent Blanc\", \"city_name\":\"Paris\", \"league_name\":\"Ligue 1\"}";
        team = CellsUtils.getCellFromJson((org.json.simple.JSONObject) JSONValue.parse(teamJson), "testTable");
        teams.add(team);

        teamJson = "{\"id\":6, \"name\":\"HJK Helsinki\", \"short_name\":\"HJK\", \"arena_name\":\"Sonera Stadium\", "
                + "\"coach_name\":\"Mika Lehkosuo\", \"city_name\":\"Helsinki\", \"league_name\":\"Veikkausliiga\"}";
        team = CellsUtils.getCellFromJson((org.json.simple.JSONObject) JSONValue.parse(teamJson), "testTable");
        teams.add(team);

        return teams;
    }
}