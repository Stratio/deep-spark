package com.stratio.deep.jdbc.extractor;

import com.stratio.deep.jdbc.reader.JdbcReader;
import com.stratio.deep.jdbc.testentity.MessageTestEntity;
import com.stratio.deep.jdbc.writer.JdbcWriter;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.any;

/**
 * Created by mariomgal on 12/12/14.
 */
@Test(groups = { "UnitTests" })
public class JdbcNativeEntityExtractorTest {

    private JdbcReader jdbcReader = PowerMockito.mock(JdbcReader.class);

    private JdbcWriter jdbcWriter = PowerMockito.mock(JdbcWriter.class);

    @Test
    public void testSaveRdd() throws Exception {
        JdbcNativeEntityExtractor extractor = createJdbcEntityExtractor();
        extractor.saveRDD(new MessageTestEntity());
        verify(jdbcWriter, times(1)).save(anyMap());
    }

    private JdbcNativeEntityExtractor createJdbcEntityExtractor() {
        JdbcNativeEntityExtractor extractor = new JdbcNativeEntityExtractor(MessageTestEntity.class);
        Whitebox.setInternalState(extractor, "jdbcReader", jdbcReader);
        Whitebox.setInternalState(extractor, "jdbcWriter", jdbcWriter);
        return extractor;
    }
}
