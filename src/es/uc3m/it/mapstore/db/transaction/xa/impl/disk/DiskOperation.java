/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.disk;

import java.io.Serializable;

/**
 *
 * @author Pablo
 */
public class DiskOperation implements Serializable{
   public static final int CREATE = 0;
    public static final int UPDATE = 1;
    public static final int DELETE = 2;

    int operation;
    Object parameters[];

    public DiskOperation(int operation, Object[] parameters) {
        this.operation = operation;
        this.parameters = parameters;
    }

    public int getOperation() {
        return operation;
    }

    public Object[] getParameters() {
        return parameters;
    }
}
