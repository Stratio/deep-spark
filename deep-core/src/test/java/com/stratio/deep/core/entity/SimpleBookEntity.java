/*
 * Copyright 2014, Stratio.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.stratio.deep.core.entity;

import java.util.List;
import java.util.Map;

import com.stratio.deep.commons.annotations.DeepEntity;
import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.IDeepType;

/**
 * Created by rcrespo on 25/06/14.
 */
@DeepEntity
public class SimpleBookEntity implements IDeepType {

    @DeepField(fieldName = "id", isPartOfPartitionKey = true, isPartOfClusterKey = true)
    private String id;

    @DeepField(fieldName = "cantos")
    private List<String> cantos;

    @DeepField(fieldName = "metadata")
    private Map<String, String> metadata;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getCantos() {
        return cantos;
    }

    public void setCantos(List<String> cantos) {
        this.cantos = cantos;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SimpleBookEntity{");
        sb.append("id=").append(id);
        sb.append(", cantos=").append(cantos);
        sb.append(", metadata=").append(metadata);
        sb.append('}');
        return sb.toString();
    }
}
