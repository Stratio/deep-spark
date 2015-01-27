package com.stratio.deep.jdbc.config;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import org.neo4j.jdbc.Driver;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Created by mariomgal on 26/01/15.
 */
@Test(groups = { "UnitTests" })
public class JdbcNeo4JDeepJobConfigTest {

    private static final String HOST = "localhost";
    private static final int PORT = 3306;
    private static final Class DRIVER_CLASS = Driver.class;
    private static final String DATABASE = "NEO4J";
    private static final String TABLE = "table";
    private static final String CYPHER_QUERY = "query";
    private static final String CONNECTION_URL = "connectionUrl";

    @Test
    public void testInitialize() throws Exception {
        JdbcNeo4JDeepJobConfig deepJobConfig = new JdbcNeo4JDeepJobConfig(Cells.class);
        deepJobConfig.initialize(getExtractorConfig());

        assertEquals(deepJobConfig.getHost(), HOST);
        assertEquals(deepJobConfig.getPort(), PORT);
        assertEquals(deepJobConfig.getDriverClass(), DRIVER_CLASS.getCanonicalName());
        assertEquals(deepJobConfig.getDatabase(), DATABASE);
        assertEquals(deepJobConfig.getTable(), TABLE);
    }

    private ExtractorConfig getExtractorConfig() {
        ExtractorConfig config = new ExtractorConfig();
        config.putValue(ExtractorConstants.HOST, HOST);
        config.putValue(ExtractorConstants.PORT, PORT);
        config.putValue(ExtractorConstants.JDBC_DRIVER_CLASS, DRIVER_CLASS);
        config.putValue(ExtractorConstants.CATALOG, DATABASE);
        config.putValue(ExtractorConstants.TABLE, TABLE);
        config.putValue(ExtractorConstants.JDBC_CONNECTION_URL, CONNECTION_URL);
        config.putValue(ExtractorConstants.JDBC_QUERY, CYPHER_QUERY);
        return config;
    }
}
