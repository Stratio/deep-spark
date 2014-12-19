/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.deep.aerospike.extractor;

import com.aerospike.client.Record;
import com.stratio.deep.aerospike.config.AerospikeDeepJobConfig;
import com.stratio.deep.aerospike.utils.UtilAerospike;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepTransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of AerospikeNativeExtractor for Cells type.
 */
public class AerospikeNativeCellExtractor extends AerospikeNativeExtractor<Cells, AerospikeDeepJobConfig<Cells>>{

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeNativeCellExtractor.class);

    private static final long serialVersionUID = 3144603151334035970L;

    /**
     * Public constructor.
     */
    public AerospikeNativeCellExtractor() {
        this.aerospikeDeepJobConfig = new AerospikeDeepJobConfig<>(Cells.class);
    }

    /**
     * Public constructor.
     * @param cellsClass Class of type Cells.
     */
    public AerospikeNativeCellExtractor(Class<Cells> cellsClass) {
        this.aerospikeDeepJobConfig = new AerospikeDeepJobConfig<>(Cells.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Cells transformElement(Record record) {
        try {
            return UtilAerospike.getCellsFromRecord(record, this.aerospikeDeepJobConfig);
        } catch (Exception e) {
            LOG.error("Cannot convert AerospikeRecord: ", e);
            throw new DeepTransformException("Could not transform from Aerospike Record to Cells " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Record transformElement(Cells cells) {
        try {
            return UtilAerospike.getRecordFromCell(cells);
        } catch (Exception e) {
            LOG.error("Cannot convert Cells: ", e);
            throw new DeepTransformException("Could not transform from Cells to Aerospike " + e.getMessage(), e);
        }
    }
}
