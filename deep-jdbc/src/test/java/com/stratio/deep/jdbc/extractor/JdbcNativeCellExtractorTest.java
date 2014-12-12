package com.stratio.deep.jdbc.extractor;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.jdbc.config.JdbcConfigFactory;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.jdbc.reader.JdbcReader;
import com.stratio.deep.jdbc.utils.UtilJdbc;
import com.stratio.deep.jdbc.writer.JdbcWriter;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * Created by mariomgal on 12/12/14.
 */
@Test(groups = { "UnitTests" })
public class JdbcNativeCellExtractorTest {

    private JdbcReader jdbcReader = PowerMockito.mock(JdbcReader.class);

    private JdbcWriter jdbcWriter = PowerMockito.mock(JdbcWriter.class);

    @Test
    public void testSaveRdd() throws Exception {
        JdbcNativeCellExtractor extractor = createJdbcNativeExtractor();
        extractor.saveRDD(new Cells());
        verify(jdbcWriter, times(1)).save(anyMap());
    }

    private JdbcNativeCellExtractor createJdbcNativeExtractor() {
        JdbcNativeCellExtractor extractor = new JdbcNativeCellExtractor();
        Whitebox.setInternalState(extractor, "jdbcReader", jdbcReader);
        Whitebox.setInternalState(extractor, "jdbcWriter", jdbcWriter);
        return extractor;
    }

}
