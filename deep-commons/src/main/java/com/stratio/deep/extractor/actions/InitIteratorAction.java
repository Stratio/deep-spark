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
package com.stratio.deep.extractor.actions;

import com.stratio.deep.config.ExtractorConfig;
import org.apache.spark.Partition;

/**
 * @author Óscar Puertas
 */
public class InitIteratorAction<T> extends Action {

    private static final long serialVersionUID = -1270097974102584045L;

    private ExtractorConfig<T> config;


    private Partition partition;

    public InitIteratorAction() {
        super();
    }

    public InitIteratorAction(Partition partition, ExtractorConfig<T> config) {
        super(ActionType.INIT_ITERATOR);
        this.config = config;
        this.partition = partition;
    }


    public ExtractorConfig<T> getConfig() {
        return config;
    }

    public Partition getPartition() {
        return partition;
    }
}
