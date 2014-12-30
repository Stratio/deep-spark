package com.stratio.deep.core.entity;

import com.stratio.deep.commons.annotations.DeepEntity;
import com.stratio.deep.commons.annotations.DeepField;
import com.stratio.deep.commons.entity.IDeepType;

/**
 * Created by rmorandeira on 29/12/14.
 */
@DeepEntity
public class TeamEntity implements IDeepType {
    private static final long serialVersionUID = 8136154843867246732L;

    @DeepField(fieldName = "id", isPartOfClusterKey = true, isPartOfPartitionKey = true)
    private Long id;

    @DeepField(fieldName = "name")
    private String name;

    @DeepField(fieldName = "short_name")
    private String shortName;

    @DeepField(fieldName = "arena_name")
    private String arenaName;

    @DeepField(fieldName = "coach_name")
    private String coachName;

    @DeepField(fieldName = "city_name")
    private String cityName;

    @DeepField(fieldName = "league_name")
    private String leagueName;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public String getArenaName() {
        return arenaName;
    }

    public void setArenaName(String arenaName) {
        this.arenaName = arenaName;
    }

    public String getCoachName() {
        return coachName;
    }

    public void setCoachName(String coachName) {
        this.coachName = coachName;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public String getLeagueName() {
        return leagueName;
    }

    public void setLeagueName(String leagueName) {
        this.leagueName = leagueName;
    }

    @Override
    public String toString() {
        return "TeamEntity{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", shortName='" + shortName + '\'' +
                ", arenaName='" + arenaName + '\'' +
                ", coachName='" + coachName + '\'' +
                ", cityName='" + cityName + '\'' +
                ", leagueName='" + leagueName + '\'' +
                '}';
    }
}
