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
 * Generic Deep exception.
 *
 * @author Luca Rosellini <luca@stratio.com>
 */
public class DeepGenericException extends RuntimeException {

    private static final long serialVersionUID = -1912500294194290036L;

    /**
     * Default constructor.
     */
    public DeepGenericException() {
        super();
    }

    /**
     * Public constructor.
     */
    public DeepGenericException(String message) {
        super(message);
    }

    /**
     * Public constructor.
     */
    public DeepGenericException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Public constructor.
     */
    public DeepGenericException(Throwable cause) {
        super(cause);
    }

}
