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

package com.stratio.deep.exception;

/**
 * Created by jmgomez on 22/08/14.
 */
public class DeepExtractorinitializationException extends Exception {

    /**
     * Default constructor.
     */
    public DeepExtractorinitializationException() {
        super();
    }

    /**
     * Public constructor.
     */
    public DeepExtractorinitializationException(String message) {
        super(message);
    }

    /**
     * Public constructor.
     */
    public DeepExtractorinitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Public constructor.
     */
    public DeepExtractorinitializationException(Throwable cause) {
        super(cause);
    }
}
