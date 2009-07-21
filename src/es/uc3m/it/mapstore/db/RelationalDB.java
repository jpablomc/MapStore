/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db;

import java.io.Serializable;
import java.sql.ResultSet;

/**
 *
 * @author Pablo
 */
public interface RelationalDB {
    public Serializable executeCreate(String sqlQuery);
    public ResultSet executeRecover(String sqlQuey);
    public void executeUpdate(String sqlQuery);
    public void executeDelete(String sqlQuery);
}
