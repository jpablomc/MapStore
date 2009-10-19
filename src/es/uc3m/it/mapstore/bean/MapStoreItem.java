/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.bean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Elemento básico de la libreria que representa a cada uno de los elementos 
 * almacenados por la aplicación
 *
 * @author Pablo
 */
public class MapStoreItem implements Serializable{
    public final static String NAME = "_NAME";
    public final static String ID = "_ID";
    public final static String TYPE = "_TYPE";
    public final static String VERSION = "_VERSION";

    Map<String,Object> properties;

    public MapStoreItem() {
        this.properties = new HashMap<String, Object>();
    }



    /**
     *
     * Devuelve el mapa con las propiedades del objeto
     *
     * @return Las propiedades del objeto
     */
    // TODO: Ver si no debería devolverse una copia (shallow) del mapa
    public Map<String,Object> getProperties() {
        return properties;
    }

    /**
     * Devuelve el nombre definido por el usuario para identificar el objeto
     * 
     * @return
     */
    // TODO: Ver si no debería escribirse una copia (shallow) del mapa
    public String getName() {
        return (String)properties.get(NAME);
    }
    /**
     * Establece el nombre definido por el usuario para identificar el objeto
     *
     * @param value Nuevo valor del nombre del objeto
     */
    public void setName(String value) {
        //TODO: Loguear el cambio cuando se modifique el valor... posiblemente
        //pueda llevar a problemas al ejecutar un update
        properties.put(NAME, value);
    }

    /**
     * Devuelve el identificador del objeto utilizado por la librería
     * 
     * @return
     */
    public Long getId() {
        return (Long)properties.get(ID);
    }
    /**
     * Establece el identificador del objeto utilizado por la librería
     *
     * @param value Nuevo valor del identificador del objeto
     */
    public void setId(long value) {
        //TODO: Loguear el cambio cuando se modifique el valor... posiblemente
        //pueda llevar a problemas al ejecutar un update
        properties.put(ID, value);
    }

    /**
     * Devuelve la versión del objeto
     *
     * @return
     */
    public int getVersion() {
        return (Integer)properties.get(VERSION);
    }
    /**
     * Establece la versión del objeto
     *
     * @param value Nuevo valor de versión del objeto
     */
    public void setVersion(int value) {
        properties.put(VERSION, value);
    }

    /**
     * Devuelve el tipo de objeto representado
     *
     * @return
     */
    public String getType() {
        return (String)properties.get(TYPE);
    }
    /**
     * Establece el tipo de objeto representado
     *
     * @param value Nuevo valor del tipo de objeto
     */
    public void setType(String value) {
        //TODO: Loguear el cambio cuando se modifique el valor... posiblemente
        //pueda llevar a problemas al ejecutar un update
        properties.put(TYPE, value);
    }
    /**
     * Devuelve la propiedad seleccionada
     *
     * @return La propiedad solicitada o null si no existe
     */
    public Object getProperty(String propertyName) {
        //TODO: Controlar el acceso a las propiedades especiales...
        //TODO: Tal vez devolver copia del objeto
        return properties.get(propertyName);
    }
    /**
     *
     * Establece el valor de la propiedad indicada
     *
     * @param propertyName Nombre de la propiedad a modificar
     * @param value Nuevo valor de la propiedad
     */
    public void setProperty(String propertyName, Object value) {
        /*TODO: Tal vez en vez de lanzar los metodos se deban escapar los nombres
         * de la propiedad para que puedan ser usados por el usuario para otros
         * menesteres
         */
        if (NAME.equals(propertyName)) setName((String)value);
        else if (ID.equals(propertyName)) setId((Long) value);
        else if (TYPE.equals(propertyName)) setType((String)value);
        else properties.put(propertyName, value);
    }
}

