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

import com.stratio.deep.annotations.DeepEntity;
import com.stratio.deep.annotations.DeepField;
import com.stratio.deep.entity.IDeepType;
import org.apache.cassandra.db.marshal.Int32Type;

@DeepEntity
public class StrippedTestEntity implements IDeepType {

    private static final long serialVersionUID = -7394476231513436262L;

    @DeepField
    private String id;

    @DeepField
    private String url;

    @DeepField(validationClass = Int32Type.class, fieldName = "response_time")
    private Integer responseTime;

    public StrippedTestEntity(TestEntity te) {
        this.id = te.getId();
        this.url = te.getUrl();
        this.responseTime = te.getResponseTime();
    }

    String getId() {
        return id;
    }

    Integer getResponseTime() {
        return responseTime;
    }

    String getUrl() {
        return url;
    }

    void setId(String id) {
        this.id = id;
    }

    void setResponseTime(Integer responseTime) {
        this.responseTime = responseTime;
    }

    void setUrl(String url) {
        this.url = url;
    }

}
