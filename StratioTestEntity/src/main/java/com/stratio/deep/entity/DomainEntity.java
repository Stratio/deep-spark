package com.stratio.deep.entity;

import com.stratio.deep.annotations.DeepEntity;
import com.stratio.deep.annotations.DeepField;

@DeepEntity
public class DomainEntity implements IDeepType {

    private static final long serialVersionUID = 7262854550753855586L;

    @DeepField
    private String domain;

    public String getDomain() {
	return domain;
    }

    public void setDomain(String domain) {
	this.domain = domain;
    }
}
