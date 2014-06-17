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

/**
 * Created by luca on 03/02/14.
 */
@DeepEntity
public class Cql3TestEntity implements IDeepType {

    /**
     *
     */
    private static final long serialVersionUID = 4248945021023974172L;

    @DeepField(isPartOfPartitionKey = true)
    private String name;

    @DeepField
    private String password;

    @DeepField
    private String color;

    @DeepField(isPartOfPartitionKey = true)
    private String gender;

    @DeepField
    private String food;

    @DeepField(isPartOfClusterKey = true)
    private String animal;

    @DeepField
    private String lucene;

    @DeepField(validationClass = Int32Type.class, isPartOfClusterKey = true)
    private Integer age;

    public Cql3TestEntity() {

    }

    public Cql3TestEntity(String name, String password, String color, String gender, String food, String animal,
                          String lucene) {
        this.name = name;
        this.password = password;
        this.color = color;
        this.gender = gender;
        this.food = food;
        this.animal = animal;
        this.lucene = lucene;
        this.age = 15;
    }

    public Integer getAge() {
        return age;
    }

    public String getAnimal() {
        return animal;
    }

    public String getColor() {
        return color;
    }

    public String getFood() {
        return food;
    }

    public String getGender() {
        return gender;
    }

    public String getLucene() {
        return lucene;
    }

    public String getName() {
        return name;
    }

    public String getPassword() {
        return password;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public void setAnimal(String animal) {
        this.animal = animal;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public void setFood(String food) {
        this.food = food;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setLucene(String lucene) {
        this.lucene = lucene;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
