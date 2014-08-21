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

import com.stratio.deep.annotations.DeepField;
import com.stratio.deep.entity.IDeepType;

/**
 * Abstract Test Entity class, used to test the system works correctly with domain entities that inherit from
 * a base class.<br/>
 * Note that in order to have all of the fields of the entity correctly serialized, the base class must be serializable.
 */
public abstract class BaseTestEntity implements IDeepType {
    @DeepField(fieldName = "domain_name")
    protected String domain;
    @DeepField(isPartOfPartitionKey = true)
    private String id;

    public BaseTestEntity(String domain) {
        this.domain = domain;
    }

    public BaseTestEntity() {

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }
}
