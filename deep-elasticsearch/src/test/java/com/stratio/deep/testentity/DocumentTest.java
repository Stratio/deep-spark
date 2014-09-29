package com.stratio.deep.testentity;

import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.IDeepType;

/**
 * Created by rcrespo on 5/08/14.
 */
public class DocumentTest implements IDeepType {

    @DeepField(fieldName = "sub1-campo1")
    private String subCampo1;
    @DeepField(fieldName = "sub1-campo2")
    private String subCampo2;

    public String getSubCampo1() {
        return subCampo1;
    }

    public void setSubCampo1(String subCampo1) {
        this.subCampo1 = subCampo1;
    }

    public String getSubCampo2() {
        return subCampo2;
    }

    public void setSubCampo2(String subCampo2) {
        this.subCampo2 = subCampo2;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("DocumentTest{");
        sb.append("subCampo1='").append(subCampo1).append('\'');
        sb.append(", subCampo2='").append(subCampo2).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
