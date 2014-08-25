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
package com.stratio.deep.extractor.response;

import com.stratio.deep.extractor.actions.ActionType;
import org.apache.spark.Partition;

/**
 * @author Óscar Puertas
 */
public class GetPartitionsResponse extends Response {

    private static final long serialVersionUID = -7728817078374511478L;

    private Partition[] partitions;

    public GetPartitionsResponse() {
        super();
    }

    public GetPartitionsResponse(Partition[] partitions) {
        super(ActionType.GET_PARTITIONS);
        this.partitions = partitions;
    }

    public Partition[] getPartitions() {
        return partitions;
    }
}
