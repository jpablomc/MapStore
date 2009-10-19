
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.db.impl.MapStoreSession;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Pablo
 */
public class Main {
    public static void main(String args[]) {
        Main m = new Main();
        m.testBeginTransaction();
        m.recoverAll();
    }

    public void testBeginTransaction() {
        MapStoreItem item = generateMapTStoreItemExample();
        System.out.println("beginTransaction");
        MapStoreSession instance = MapStoreSession.getSession();
        instance.beginTransaction();
        instance.save(item);
        instance.commit();
        instance.close();
    }

    private MapStoreItem generateMapTStoreItemExample() {
        MapStoreItem item = new MapStoreItem();
        item.setName("Prueba2");
        item.setType("Any");
        item.setProperty("prop1", Long.valueOf("10"));
        item.setProperty("prop2", Integer.valueOf("10"));
        return item;
    }

    public void recoverAll() {
        MapStoreSession instance = MapStoreSession.getSession();
        instance.beginTransaction();
        try {
        instance.getAll();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            instance.close();
        }
    }
}
