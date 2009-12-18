/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.bean;

import java.util.Date;

/**
 * This class is a wrapper for a recovered object from the database. In adition
 * to the object provides additional info that is lost when the object is recovered
 *
 * @author Pablo
 */
public class MapStoreExtendedItem<T extends Object> {
    private String name;
    private Integer id;
    private Integer version;
    private Date recordDate;
    private Boolean deleted;
    private T value;

    public MapStoreExtendedItem(String name, Integer id, Integer version, Date recordDate, Boolean deleted, T value) {
        this.name = name;
        this.id = id;
        this.version = version;
        this.recordDate = recordDate;
        this.deleted = deleted;
        this.value = value;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public Integer getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Date getRecordDate() {
        return recordDate;
    }

    public T getValue() {
        return value;
    }

    public Integer getVersion() {
        return version;
    }


}
