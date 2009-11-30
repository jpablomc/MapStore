/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.impl;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.LazyLoader;

/**
 *
 * @author Pablo
 */
public class LazyObject implements LazyLoader{
    private int id;
    private int version;
    private Class clazz;

    public LazyObject(int id, int version, Class clazz) {
        this.id = id;
        this.version = version;
        this.clazz = clazz;
    }

    public static <T> T newInstance(int id, int version, Class<T> clazz) {
        LazyObject interceptor = new LazyObject(id, version, clazz);
        Enhancer e = new Enhancer();
        e.setSuperclass(clazz);
        e.setCallback(interceptor);
        Object bean = e.create();
        return (T)bean;
    }

    @Override
    public Object loadObject() throws Exception {
        MapStoreSession s = MapStoreSession.getSession();
        return s.recoverByIdVersion(id, version, clazz);
    }

}
