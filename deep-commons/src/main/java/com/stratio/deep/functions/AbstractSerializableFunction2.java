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

package com.stratio.deep.functions;

import scala.runtime.AbstractFunction2;

import java.io.Serializable;

/**
 * Created by luca on 14/04/14.
 */
public abstract class AbstractSerializableFunction2<T1, T2, R> extends AbstractFunction2<T1, T2,
        R> implements Serializable {

}
