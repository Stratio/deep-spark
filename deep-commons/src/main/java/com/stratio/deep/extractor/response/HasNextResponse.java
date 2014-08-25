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

/**
 * @author Óscar Puertas
 */
public class HasNextResponse extends Response {

    private static final long serialVersionUID = -2647516898871636731L;

    private boolean data;

    public HasNextResponse() {
        super();
    }

    public HasNextResponse(boolean data) {
        super(ActionType.HAS_NEXT);
        this.data = data;
    }

    public boolean getData() {
        return data;
    }
}
