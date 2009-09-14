/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uc3m.it.mapstore.config;

import es.uc3m.it.mapstore.db.transaction.TransactionManagerWrapper;
import es.uc3m.it.mapstore.db.transaction.xa.ResourceManagerWrapper;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import es.uc3m.it.mapstore.transformers.factory.TransformerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.configuration.tree.NodeCombiner;
import org.apache.commons.configuration.tree.OverrideCombiner;

/**
 *
 * @author Pablo
 */
public class MapStoreConfig {

    public static final String TXMANAGER = "TX_MANAGER";
    public static final String RESOURCE_LOOKUP = "RESOURCE_LOOKUP";
    public static final String PERSISTANCE_LOOKUP = "PERSISTANCE_LOOKUP";
    public static final String TYPES_LIST = "TYPES_LIST";
    public static final String TRANSFORMERFACTORY = "TRANSFORMER_FACTORY";
    private static final String TXMANAGER_PATH = "transaction.transaction_manager";
    private static final String TXMANAGER_FACTORY = ".factory_class";
    private static final String TXMANAGER_PROPERTIES = ".props";
    private static final String XA_PROPERTIES = "transaction.xa";
    private static final String XA_NAME = "name";
    private static final String XA_TYPE = "type";
    private static final String XA_FACTORY = "factory";
    private static final String XA_PROP = "prop";
    private static final String TYPES_PATH = "types";
    private static final String TYPE_PATH = "type";
    private static final String TYPE_DEFAULT = "default";
    private static final String TYPE_CLASS = "class";
    private static final String TYPE_RESOURCE = "resource_type";
    private static final String TRANSFORMER_PATH = "transformer_factory";
    private static final String PERSISTANCE_PATH = "transaction.persistance";

    private static MapStoreConfig config;
    private HierarchicalConfiguration properties;

    private MapStoreConfig() {
        NodeCombiner combiner = new OverrideCombiner();

        CombinedConfiguration  conf = new CombinedConfiguration (combiner);
        properties = conf;
        //Se inserta primero la de mayor prioridad
        //TODO:Añadir el fichero
        //conf.addConfiguration(new PropertiesConfiguration());
        //conf.addConfiguration(new XMLConfiguration());
        conf.addConfiguration(getDefaultConfiguration());
        createConfigObjects();
    }

    private AbstractConfiguration getDefaultConfiguration() {
        try {
            XMLConfiguration c = new XMLConfiguration("default.xml");
            return c;
        } catch (ConfigurationException ex) {
            Logger.getLogger(MapStoreConfig.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException("Can not find default configuration", ex);
        }
    }

    private AbstractConfiguration getPropertiesConfiguration() {
        PropertiesConfiguration prop = null;
        try {
            prop = new PropertiesConfiguration("mapstore.properties");
        } catch (ConfigurationException ex) {
            Logger.getLogger(MapStoreConfig.class.getName()).log(Level.SEVERE, null, ex);
        }
        return prop;
    }

    private AbstractConfiguration getXMLConfiguration() {
        try {
            XMLConfiguration c = new XMLConfiguration("mapstore.xml");
            return c;
        } catch (ConfigurationException ex) {
            Logger.getLogger(MapStoreConfig.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException("Can not find default configuration", ex);
        }
    }


    public static MapStoreConfig getInstance() {
        if (config == null) {
            config = new MapStoreConfig();
        }
        return config;
    }

    private void setXMLProperties() {
        //TODO: Implementar
    }

    private void setPropFileProperties() {
        //TODO: Implementar
    }

    public String getProperty(String property) {
        return properties.getString(property);
    }

    public Object getObject(String property) {
        return properties.getProperty(property);
    }

    private void createConfigObjects() {
        //Localizar el txManager
        registerDBResources();
        registerTransactionManager();
        registerTypesAssociation();
        registerTransformerFactory();
    }

    private void  registerTransactionManager() throws MapStoreRunTimeException{
        String def = "es.uc3m.it.mapstore.db.transaction.NeoTransactionManagerLookup";
        String tmlClass = properties.getString(TXMANAGER_PATH + TXMANAGER_FACTORY);
        Object v = null;
        try {
            v = createInstance(TransactionManagerWrapper.class, tmlClass, def, TXMANAGER_PATH + TXMANAGER_FACTORY);
        } catch (MapStoreRunTimeException ex) {
            throw new MapStoreRunTimeException("Can not instantiate Tranasction Manager Lookup",ex);
        }
        TransactionManagerWrapper tm = (TransactionManagerWrapper)v;
        properties.setProperty(TXMANAGER, tm);
    }

    public Properties getTransactionManagerProperties() {
        SubnodeConfiguration snc = properties.configurationAt(TXMANAGER_PATH + TXMANAGER_PROPERTIES);
        return ConfigurationConverter.getProperties(snc);
    }

    private void registerDBResources() throws MapStoreRunTimeException{
       int index = 0;
       String key = XA_PROPERTIES;
       HierarchicalConfiguration sub = properties.configurationAt(key+ "("+index+")");
       Map<String,ResourceManagerWrapper> resources = new HashMap<String,ResourceManagerWrapper>();
       while(sub != null) {
           String f = sub.getString(XA_FACTORY);
           String n = sub.getString(XA_NAME);
           ResourceManagerWrapper rl = (ResourceManagerWrapper) createInstance(ResourceManagerWrapper.class, f, null, null);
           rl.start(ConfigurationConverter.getProperties(sub));
           resources.put(n, rl);
           index++;
           try {
           sub = properties.configurationAt(key+ "("+index+")");
           } catch (IllegalArgumentException e) {
               //No hay mas fuentes definidas
               sub= null;
           }
       }
       properties.setProperty(RESOURCE_LOOKUP, resources);
    }

    private void registerTypesAssociation() {
       int index = 0;
       String key = TYPES_PATH + "." + TYPE_PATH;
       HierarchicalConfiguration sub = properties.configurationAt(key+ "("+index+")");
       List<ClassResourceType> types = new ArrayList<ClassResourceType>();
       while(sub != null) {
           String c = sub.getString(TYPE_CLASS);
           String rt = sub.getString(TYPE_RESOURCE);
           Class clazz;
            try {
                clazz = Class.forName(c);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(MapStoreConfig.class.getName()).log(Level.SEVERE, null, ex);
                throw new MapStoreRunTimeException("Class not found exception",ex);
            }
           types.add(new ClassResourceType(clazz,rt));
           index++;
           try {
           sub = properties.configurationAt(key+ "("+index+")");
           } catch (IllegalArgumentException e) {
               //No hay mas fuentes definidas
               sub= null;
           }
       }
       key = TYPES_PATH + "." + TYPE_DEFAULT;
       sub = properties.configurationAt(key);
       String rt = sub.getString(TYPE_RESOURCE);
       types.add(new ClassResourceType(Object.class, rt));
       properties.setProperty(TYPES_LIST, types);

    }

    private void registerTransformerFactory() {
        Exception e = null;
        try {
            String clazzName = properties.getString(TRANSFORMER_PATH);
            TransformerFactory tf = (TransformerFactory) Class.forName(clazzName).newInstance();
            properties.addProperty(TRANSFORMERFACTORY, tf);
        } catch (ClassNotFoundException ex) {
            e = ex;
            Logger.getLogger(MapStoreConfig.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            e = ex;
            Logger.getLogger(MapStoreConfig.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            e = ex;
            Logger.getLogger(MapStoreConfig.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (e != null) throw new MapStoreRunTimeException(e);
    }

    private void print(ConfigurationNode cn, int depth) {
        StringBuffer sb = new StringBuffer();
        for (int i= 0;i<depth;i++) sb.append("\t");
        System.out.println(sb.toString() + cn.getName() + cn.getValue());

        List<ConfigurationNode> children = cn.getChildren();
        for (ConfigurationNode c : children) {
            print(c,depth+1);
        }
    }

    private Object createInstance(Class interfaz, String className,String def,String key) {
        Object v = null;
        try {           
            if (className == null && def != null) {
                className = def;
                if (key != null) properties.setProperty(key, className);                   
            }
            if (!(interfaz.isAssignableFrom(Class.forName(className)))) {
                //TODO: Internacionalizar
                Logger.getLogger(MapStoreConfig.class.getName()).log(Level.SEVERE, "Unable to convert class with name " + className + "to " + interfaz.getName());
                throw new MapStoreRunTimeException("Unable to convert class with name " + className + "to " + interfaz.getName());
            }
            v = Class.forName(className).newInstance();

        } catch (InstantiationException ex) {
            Logger.getLogger(MapStoreConfig.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException("Unable to create " + className);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(MapStoreConfig.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException("Unable to create " + className + ". Illegal Access");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MapStoreConfig.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException("Unable to create " + className + ".Class not found");
        }
        return v;
    }

    public Collection<ResourceManagerWrapper> getXAResourceLookup() {
        Map<String,ResourceManagerWrapper> res = (Map<String,ResourceManagerWrapper>)properties.getProperty(RESOURCE_LOOKUP);
        return res.values();
    }

    private class ClassResourceType {
        private Class clazz;
        private String resourceType;

        private ClassResourceType(Class clazz, String resourceType) {
            this.clazz = clazz;
            this.resourceType = resourceType;
        }

        public Class getClazz() {
            return clazz;
        }

        public void setClazz(Class clazz) {
            this.clazz = clazz;
        }

        public String getResourceType() {
            return resourceType;
        }

        public void setResourceType(String resourceType) {
            this.resourceType = resourceType;
        }
    }

    public List<ResourceManagerWrapper> getXaResourceLookupForClass(Class c) {
        List<ResourceManagerWrapper> list = new ArrayList<ResourceManagerWrapper>();
        List<ClassResourceType> types = (List<ClassResourceType>)properties.getProperty(TYPES_LIST);
        boolean notFound = true;
        String resourceType = null;
        Iterator<ClassResourceType> it = types.iterator();
        while (notFound && it.hasNext()) {
            ClassResourceType crt = it.next();
            if (crt.getClazz().isAssignableFrom(c)) {
                resourceType = crt.getResourceType();
                notFound = false;
            }
        }
        if (resourceType == null) {

        }
        if (resourceType != null) {
            Collection<ResourceManagerWrapper> values  = ((Map<String,ResourceManagerWrapper>)properties.getProperty(RESOURCE_LOOKUP)).values();
            for (ResourceManagerWrapper xa : values) {
                if (resourceType.equals(xa.getType())) {
                    list.add(xa);
                }
            }
        }
        return list;
    }

    public TransformerFactory getTransformerFactory() {
        return (TransformerFactory) properties.getProperty(TRANSFORMERFACTORY);
    }
}
