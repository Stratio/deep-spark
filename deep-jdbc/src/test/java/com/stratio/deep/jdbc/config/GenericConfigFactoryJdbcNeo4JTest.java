package com.stratio.deep.jdbc.config;

import com.mysql.jdbc.Driver;
import com.stratio.deep.commons.entity.Cells;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

/**
 * Created by mariomgal on 26/01/15.
 */
@Test(groups = { "UnitTests" })
public class GenericConfigFactoryJdbcNeo4JTest {

    private static final String HOST = "localhost";

    private static final int PORT = 3306;

    private static final String CONNECTION_URL = "jdbc:neo4j://localhost/neo4j";

    private static final String CYPHER_QUERY = "MATCH (a)-[:`ACTED_IN`]->(b) RETURN a,b LIMIT 25";

    @Test
    public void testConnectionUrlValidation() {
        JdbcNeo4JDeepJobConfig<Cells> config = JdbcNeo4JConfigFactory.createJdbcNeo4J();
        config.host(HOST).port(PORT).cypherQuery(CYPHER_QUERY);
        try {
            config.initialize();
            fail();
        } catch(IllegalArgumentException e) {
            config.connectionUrl(CONNECTION_URL);
        }
        config.initialize();
    }

    @Test
    public void testCypherQueryValidation() {
        JdbcNeo4JDeepJobConfig<Cells> config = JdbcNeo4JConfigFactory.createJdbcNeo4J();
        config.host(HOST).port(PORT).connectionUrl(CONNECTION_URL);
        try {
            config.initialize();
            fail();
        } catch(IllegalArgumentException e) {
            config.cypherQuery(CYPHER_QUERY);
        }
        config.initialize();
    }

}
