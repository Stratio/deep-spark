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
 * Author: Luca Rosellini
 */

@DeepEntity
public class DomainEntity implements IDeepType {

	private static final long serialVersionUID = 7262854550753855586L;

	@DeepField(fieldName = "domain", isPartOfPartitionKey = true)
	private String domain;

    @DeepField(fieldName="num_pages", validationClass = Int32Type.class)
    private Integer numPages;

	public String getDomain() {



        return domain;
	}

    public int getNumPages() {
        return numPages;
    }

	public void setDomain(String domain) {
		this.domain = domain;
	}

    public void setNumPages(Integer numPages) {
        this.numPages = numPages;
    }
}
