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

package com.stratio.deep.mongodb.entity;

import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.mongodb.utils.UtilMongoDB;

/**
 * Created by rcrespo on 2/07/14.
 */
public class MongoCell extends Cell {
    private MongoCell(String cellName, Object cellValue) {
        super(cellName, cellValue);
    }


    public Boolean isKey() {
        return cellName.equals(UtilMongoDB.MONGO_DEFAULT_ID);
    }

    public static Cell create(String cellName, Object cellValue) {
        return new MongoCell(cellName, cellValue);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MongoCell cell = (MongoCell) o;

        return this.cellName.equals(cell.cellName)&&this.cellValue.equals(cell.cellValue);
    }
}
