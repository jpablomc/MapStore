/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uc3m.it.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Pablo
 */
public class ReflectionUtils {

    public static Class determineGenericType(Collection<? extends Object> col) {
        Class toReturn = null;
        Set<Class> clazz = null;
        List<Class> clazzInterfaz = null;
        for (Object o : col) {
            if (o != null) {
                Set<Class> tmpClazz = new HashSet<Class>();
                Class c = o.getClass();
                while (c != null) {
                    tmpClazz.add(c);
                    c = c.getSuperclass();
                }
                if (clazz == null) {
                    clazz = tmpClazz;
                } else {
                    clazz.retainAll(tmpClazz);
                }
                Class[] tmpInterfazArray = o.getClass().getInterfaces();
                if (tmpInterfazArray != null) {
                    List<Class> tmpInterfaz = Arrays.asList();
                    if (clazzInterfaz == null) {
                        clazzInterfaz = tmpInterfaz;
                    } else {
                        clazzInterfaz.retainAll(tmpInterfaz);
                    }
                } else {
                    clazzInterfaz = new ArrayList<Class>();
                }
            }
        }
        if (clazz != null) {
            //clazz contiene todas las superclases comunes... determinar la más especifica de todas
            for (Class c : clazz) {
                if (toReturn == null) {
                    toReturn = c;
                } else if (toReturn.isAssignableFrom(c)) {
                    toReturn = c;
                }
            }
            //No hay clase común (solo Object) mirar herencia en interfaz
            if (toReturn.equals(Object.class)) {
                Set<Class> toDelete = new HashSet<Class>();
                for (int i = 0; i < clazzInterfaz.size() - 1; i++) {
                    for (int j = i + 1; j < clazzInterfaz.size(); j++) {
                        Class c1 = clazzInterfaz.get(i);
                        Class c2 = clazzInterfaz.get(j);
                        if (!c1.isAssignableFrom(c2) && !c2.isAssignableFrom(c1)) {
                            toDelete.add(c1);
                            toDelete.add(c2);
                        }
                    }
                }
                clazzInterfaz.removeAll(toDelete);
                //Solo quedan las que estan en herencia
                for (Class c : clazz) {
                    if (toReturn == null) {
                        toReturn = c;
                    } else if (toReturn.isAssignableFrom(c)) {
                        toReturn = c;
                    }
                }
            }
        } else {
            toReturn = Object.class;
        }        
        return toReturn;
    }
}
