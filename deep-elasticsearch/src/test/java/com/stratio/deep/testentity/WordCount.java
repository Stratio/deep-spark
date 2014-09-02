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

/**
 * Created by rcrespo on 25/06/14.
 */
@DeepEntity
public class WordCount implements IDeepType {

    @DeepField(fieldName = "_id")
    private String id;

    @DeepField
    private String word;

    @DeepField
    private Double count;



    public WordCount() {

    }

    public WordCount(String word, Double count) {
        this.word = word;
        this.count = count;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Double getCount() {
        return count;
    }

    public void setCount(Double count) {
        this.count = count;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WordCount{");
        sb.append("word='").append(word).append('\'');
        sb.append(", count=").append(count);
        sb.append('}');
        return sb.toString();
    }
}
