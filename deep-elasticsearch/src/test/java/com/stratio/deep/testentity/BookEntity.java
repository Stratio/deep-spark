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

package com.stratio.deep.testentity;

import com.stratio.deep.commons.annotations.DeepEntity;
import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.IDeepType;

import java.util.List;

/**
 * Created by rcrespo on 25/06/14.
 */
@DeepEntity
public class BookEntity implements IDeepType {

    @DeepField(fieldName = "_id")
    private String id;

    @DeepField(fieldName = "cantos")
    private List<CantoEntity> cantoEntities;

    @DeepField(fieldName = "metadata")
    private MetadataEntity metadataEntity;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }


    public List<CantoEntity> getCantoEntities() {
        return cantoEntities;
    }

    public void setCantoEntities(List<CantoEntity> cantoEntities) {
        this.cantoEntities = cantoEntities;
    }

    public MetadataEntity getMetadataEntity() {
        return metadataEntity;
    }

    public void setMetadataEntity(MetadataEntity metadataEntity) {
        this.metadataEntity = metadataEntity;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BookEntity{");
        sb.append("id=").append(id);
        sb.append(", cantoEntities=").append(cantoEntities);
        sb.append(", metadataEntity=").append(metadataEntity);
        sb.append('}');
        return sb.toString();
    }
}
