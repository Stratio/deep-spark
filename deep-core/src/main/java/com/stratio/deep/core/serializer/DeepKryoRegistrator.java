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

package com.stratio.deep.core.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.entity.IDeepType;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * Generic kryo registrator we provide to end users.
 */
public class DeepKryoRegistrator implements KryoRegistrator {
    /**
     * {@inheritDoc}
     */
    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(Cell.class);
        kryo.register(Cells.class);
        kryo.register(IDeepType.class);
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
    }
}
