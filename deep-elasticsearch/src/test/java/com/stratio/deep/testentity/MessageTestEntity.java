package com.stratio.deep.testentity;

import com.stratio.deep.annotations.DeepEntity;
import com.stratio.deep.annotations.DeepField;
import com.stratio.deep.entity.IDeepType;

/**
 * Created by rcrespo on 18/06/14.
 */
@DeepEntity
public class MessageTestEntity implements IDeepType {

    @DeepField(fieldName = "_id")
    private String id;

    @DeepField
    private String message;


}
