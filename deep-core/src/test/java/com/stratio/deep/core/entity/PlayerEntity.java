package com.stratio.deep.core.entity;

import com.stratio.deep.commons.annotations.DeepEntity;
import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.IDeepType;

/**
 * Created by rmorandeira on 29/12/14.
 */
@DeepEntity
public class PlayerEntity implements IDeepType {
    private static final long serialVersionUID = -8236296091248740035L;

    @DeepField(fieldName = "id", isPartOfPartitionKey = true, isPartOfClusterKey = true)
    private Long id;

    @DeepField(fieldName = "firstname")
    private String firstName;

    @DeepField(fieldName = "lastname")
    private String lastName;

    @DeepField(fieldName = "date_of_birth")
    private String dateOfBirth;

    @DeepField(fieldName = "place_of_birth")
    private String placeOfBirthName;

    @DeepField(fieldName = "position_name")
    private String positionName;

    @DeepField(fieldName = "team_id")
    private Long teamId;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getDateOfBirth() {
        return dateOfBirth;
    }

    public void setDateOfBirth(String dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    public String getPlaceOfBirthName() {
        return placeOfBirthName;
    }

    public void setPlaceOfBirthName(String placeOfBirthName) {
        this.placeOfBirthName = placeOfBirthName;
    }

    public String getPositionName() {
        return positionName;
    }

    public void setPositionName(String positionName) {
        this.positionName = positionName;
    }

    public Long getTeamId() {
        return teamId;
    }

    public void setTeamId(Long teamId) {
        this.teamId = teamId;
    }

    @Override
    public String toString() {
        return "PlayerEntity{" +
                "id='" + id + '\'' +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", dateOfBirth=" + dateOfBirth +
                ", placeOfBirthName='" + placeOfBirthName + '\'' +
                ", positionName='" + positionName + '\'' +
                ", teamId=" + teamId +
                '}';
    }
}
