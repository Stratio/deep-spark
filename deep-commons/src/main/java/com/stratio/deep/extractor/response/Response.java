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
package com.stratio.deep.extractor.response;

import com.stratio.deep.extractor.actions.ActionType;

import java.io.Serializable;


/**
 * @author Óscar Puertas
 */
public abstract class Response implements Serializable {

    private static final long serialVersionUID = -3525560371269242119L;

    protected ActionType type;

    protected Response() {
        super();
    }

    public Response(ActionType type) {
        super();
        this.type = type;
    }

    public ActionType getType() {
        return type;
    }
}
