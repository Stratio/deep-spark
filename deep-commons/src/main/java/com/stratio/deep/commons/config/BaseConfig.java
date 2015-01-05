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

package com.stratio.deep.commons.config;

import static com.stratio.deep.commons.utils.Utils.cloneObjectWithParents;

import java.io.Serializable;

import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.exception.DeepInstantiationException;

/**
 * Created by rcrespo on 13/10/14.
 */
public class BaseConfig<T, S extends BaseConfig> implements Serializable, Cloneable {

    private static final long serialVersionUID = -6575509538810299996L;

    protected Class<T> entityClass;


    protected Class extractorImplClass;

    protected String extractorImplClassName;

    protected int rddId;

    protected int partitionId;

    public BaseConfig(Class<T> t) {
        this.entityClass = t;
    }

    public BaseConfig() {
        entityClass = (Class<T>) Cells.class;
    }

    public Class getExtractorImplClass() {
        return extractorImplClass;
    }

    public void setExtractorImplClass(Class extractorImplClass) {
        this.extractorImplClass = extractorImplClass;
        this.extractorImplClassName = extractorImplClass!=null?extractorImplClass.getName():null;
    }

    public Class getEntityClass() {
        return entityClass;
    }

    public void setEntityClass(Class entityClass) {
        this.entityClass = entityClass;
    }

    public String getExtractorImplClassName() {
        return extractorImplClassName;
    }

    public void setExtractorImplClassName(String extractorImplClassName) {
        this.extractorImplClassName = extractorImplClassName;
    }

    public void setExtractorImplClassName(ExtractorType extractorType) {
        this.extractorImplClassName = extractorType.getValue();
    }

    public int getRddId() {
        return rddId;
    }

    public void setRddId(int rddId) {
        this.rddId = rddId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public S clone(){
        try {
            return (S) cloneObjectWithParents(this);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new DeepInstantiationException("It was not possible to clone the object",e);
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("BaseConfig{");
        sb.append("entityClass=").append(entityClass);
        sb.append(", extractorImplClass=").append(extractorImplClass);
        sb.append(", extractorImplClassName='").append(extractorImplClassName).append('\'');
        sb.append(", rddId=").append(rddId);
        sb.append(", partitionId=").append(partitionId);
        sb.append('}');
        return sb.toString();
    }
}
