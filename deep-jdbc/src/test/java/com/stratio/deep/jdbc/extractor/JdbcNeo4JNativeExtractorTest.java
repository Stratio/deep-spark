package com.stratio.deep.jdbc.extractor;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.jdbc.config.JdbcConfigFactory;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.jdbc.reader.JdbcReader;
import com.stratio.deep.jdbc.writer.JdbcWriter;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.neo4j.jdbc.Driver;
import org.powermock.api.mockito.PowerMockito;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Created by mariomgal on 26/01/15.
 */
@Test(groups = { "UnitTests" })
public class JdbcNeo4JNativeExtractorTest {

    private static final String HOST = "localhost";
    private static final int PORT = 3306;
    private static final Class DRIVER_CLASS = Driver.class;
    private static final String DATABASE = "NEO4J";
    private static final String TABLE = "table";

    private JdbcReader jdbcReader = PowerMockito.mock(JdbcReader.class);

    private JdbcWriter jdbcWriter = PowerMockito.mock(JdbcWriter.class);

    @Test
    public void testHasNext() throws Exception {
        JdbcNativeExtractor extractor = createJdbcNativeExtractor();
        extractor.hasNext();
        verify(jdbcReader, times(1)).hasNext();
    }

    @Test
    public void testSaveRdd() throws Exception {
        JdbcNativeExtractor extractor = createJdbcNativeExtractor();
        doReturn(new HashMap()).when(extractor).transformElement(any(Object.class));
        extractor.saveRDD(new Object());
        verify(jdbcWriter, times(1)).save(any(Map.class));
    }

    @Test
    public void testClose() throws Exception {
        JdbcNativeExtractor extractor = createJdbcNativeExtractor();
        extractor.close();
        verify(jdbcWriter, times(1)).close();
        verify(jdbcReader, times(1)).close();
    }

    private JdbcNativeExtractor createJdbcNativeExtractor() {
        JdbcNativeExtractor extractor = PowerMockito.mock(JdbcNativeExtractor.class, Mockito.CALLS_REAL_METHODS);
        Whitebox.setInternalState(extractor, "jdbcDeepJobConfig", new JdbcDeepJobConfig<>(Cells.class));
        Whitebox.setInternalState(extractor, "jdbcReader", jdbcReader);
        Whitebox.setInternalState(extractor, "jdbcWriter", jdbcWriter);
        return extractor;
    }

}
