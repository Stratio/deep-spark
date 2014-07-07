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

package com.stratio.deep.entity;

import org.apache.cassandra.db.marshal.AbstractType;

import java.nio.ByteBuffer;

/**
 * Created by rcrespo on 2/07/14.
 */
public abstract class GenericCell implements Cell{

    private static final long serialVersionUID = 2298549804049316156L;

    /**
     * Name of the cell. Mapped to a DataBase column name.
     */
    protected String cellName;

    /**
     * Cell value.
     */
    protected Object cellValue;


    protected GenericCell (){
        super();
    }

    protected GenericCell (String cellName, Object cellValue){
        super();
        this.cellName = cellName;
        this.cellValue = cellValue;
    }


    public String getCellName() {
        return cellName;
    }

    public void setCellName(String cellName) {
        this.cellName = cellName;
    }

    public Object getCellValue() {
        return cellValue;
    }




    public void setCellValue(Object cellValue) {
        this.cellValue = cellValue;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Cell{");
        sb.append("cellName='").append(cellName).append('\'');
        sb.append(", cellValue=").append(cellValue);
        sb.append('}');
        return sb.toString();
    }

}
