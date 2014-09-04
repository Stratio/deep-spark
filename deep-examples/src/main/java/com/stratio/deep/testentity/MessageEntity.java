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
import org.bson.types.ObjectId;


@DeepEntity
public class MessageEntity implements IDeepType {

    private static final long serialVersionUID = 7262854550753855586L;

    @DeepField(fieldName = "_id", isPartOfPartitionKey = true)
    private ObjectId id;


    @DeepField(fieldName = "text")
    private String text;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }



    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MessageEntity{");
        sb.append("id=").append(id);
        sb.append(", text='").append(text).append('\'');
        sb.append('}');
        return sb.toString();
    }
}

