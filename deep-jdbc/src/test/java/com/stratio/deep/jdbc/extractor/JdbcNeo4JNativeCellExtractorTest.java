package com.stratio.deep.jdbc.extractor;

import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.jdbc.reader.IJdbcReader;
import com.stratio.deep.jdbc.reader.JdbcNeo4JReader;
import com.stratio.deep.jdbc.writer.IJdbcWriter;
import com.stratio.deep.jdbc.writer.JdbcNeo4JWriter;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Created by mariomgal on 26/01/15.
 */
@Test(groups = { "UnitTests" })
public class JdbcNeo4JNativeCellExtractorTest {

    private IJdbcReader jdbcReader = PowerMockito.mock(JdbcNeo4JReader.class);

    private IJdbcWriter jdbcWriter = PowerMockito.mock(JdbcNeo4JWriter.class);

    @Test
    public void testSaveRdd() throws Exception {
        JdbcNeo4JNativeCellExtractor extractor = createJdbcNativeExtractor();
        extractor.saveRDD(new Cells());
        verify(jdbcWriter, times(1)).save(anyMap());
    }

    private JdbcNeo4JNativeCellExtractor createJdbcNativeExtractor() {
        JdbcNeo4JNativeCellExtractor extractor = new JdbcNeo4JNativeCellExtractor();
        Whitebox.setInternalState(extractor, "jdbcReader", jdbcReader);
        Whitebox.setInternalState(extractor, "jdbcWriter", jdbcWriter);
        return extractor;
    }

}
