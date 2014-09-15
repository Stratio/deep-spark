package com.stratio.deep.extractor.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.DeflateSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.stratio.deep.entity.Cell;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.entity.IDeepType;
import com.stratio.deep.extractor.actions.*;
import com.stratio.deep.extractor.response.*;
import de.javakaffee.kryoserializers.DateSerializer;
import de.javakaffee.kryoserializers.UUIDSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;

import java.util.Date;
import java.util.LinkedList;
import java.util.UUID;

/**
 * Created by darroyo on 4/09/14.
 */
public class KryoRegister {
    private KryoRegister(){};

    public static void registerAction(Kryo kryo){
        kryo.register(Action.class);
        kryo.register(CloseAction.class);
        kryo.register(ExtractorInstanceAction.class);
        kryo.register(GetPartitionsAction.class);
        kryo.register(HasNextAction.class);
        kryo.register(InitIteratorAction.class);
        kryo.register(InitSaveAction.class);
        kryo.register(SaveAction.class);

        UnmodifiableCollectionsSerializer.registerSerializers(kryo);

    }

    public static void registerResponse(Kryo kryo){
     /*  kryo.register(Cell.class);
        kryo.register(Cells.class);
        kryo.register(IDeepType.class);
        kryo.register(HasNextElement.class);
        */
        kryo.register(Response.class);
        kryo.register(ExtractorInstanceResponse.class);;
        kryo.register(GetPartitionsResponse.class);
        kryo.register(HasNextResponse.class);
        kryo.register(InitIteratorResponse.class);
        kryo.register(InitSaveResponse.class);
        kryo.register(SaveResponse.class);

        kryo.register(LinkedList.class);


        registerUtils(kryo);

        UnmodifiableCollectionsSerializer.registerSerializers(kryo);

    }

    private static void registerUtils(Kryo kryo) {
        kryo.register(UUID.class, new UUIDSerializer());
    }
}
