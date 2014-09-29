package com.stratio.deep.testentity;

import com.stratio.deep.commons.annotations.DeepEntity;
import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.IDeepType;

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
