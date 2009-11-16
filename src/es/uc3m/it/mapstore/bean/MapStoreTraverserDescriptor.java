/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class describes a search on a grpah oriented databse. A route is defined
 * by a serie of properties to traverse, the minimum and maximum number of hops 
 * to produce a result, the direction that the relation have between two nodes.
 *
 * This class will assume circular properties for a route. So if the search distance
 * is larger than the indicated route circular properties will be used to determinate
 * which property must be followed as well as wich direction.
 * 
 * By desing the maximum number of hops is given by Integer.MAX_VALUE
 *
 * @author Pablo
 */
public class MapStoreTraverserDescriptor {
    public final static int DEPTH_TRAVERSER = 0;
    public final static int BREADTH_TRAVERSER = 1;
    public final static int DIRECTION_FROM_FIRST_TO_SECOND = 2;
    public final static int DIRECTION_FROM_SECOND_TO_FIRST = 3;
    public final static int DIRECTION_ANY = 4;
    public final static String ROUTE_ANY = "_ANY";

    private static int defaultSearchAlgorithm;

    private Set<Integer> initialNodes;
    private List<String> route;
    private List<Integer> directions;
    private Integer distanceMin;
    private Integer distanceMax;
    private int search_type;

    public static void setDefaultSearchAlgorithm(int defaultSearchAlgorithm) {
        switch (defaultSearchAlgorithm) {
            case DEPTH_TRAVERSER:
            case BREADTH_TRAVERSER:
                MapStoreTraverserDescriptor.defaultSearchAlgorithm = defaultSearchAlgorithm;
                break;
            default:
                throw new IllegalArgumentException("Unrecognized default search algorithm");
        }

    }

    /**
     *
     * This method will construct a traverser that followa always the given
     * property, for up to the specified distance.
     * The admited direction will be outgoing. The default search algorithm
     * search will be used
     *
     * @param property The property to follow
     * @param distance The maximum distance that can be trasversed. If NULL the
     * distance is limitless.
     */
    public MapStoreTraverserDescriptor(Set<Integer> nodes,String property, Integer distance) {
        initialNodes = nodes;
        route = new ArrayList<String>();
        directions = new ArrayList<Integer>();
        route.add(property);
        directions.add(DIRECTION_FROM_FIRST_TO_SECOND);
        this.distanceMin = 0;
        if (distance == null) this.distanceMax = Integer.MAX_VALUE;
        else this.distanceMax = distance;
        search_type = defaultSearchAlgorithm;
    }

    /**
     *
     * This method will construct a traverser that followa the route indicated
     * by property, for up to the specified distance.
     * The admited direction will be outgoing. The default search algorithm
     * search will be used
     *
     * @param property The route to follow
     * @param distance The maximum distance that can be trasversed. If NULL the
     * distance is limitless.
     */
    public MapStoreTraverserDescriptor(Set<Integer> nodes, List<String> property, Integer distance) {
        initialNodes = nodes;
        route = new ArrayList<String>(property);
        directions = new ArrayList<Integer>();
        while (directions.size()< route.size()) directions.add(DIRECTION_FROM_FIRST_TO_SECOND);
        this.distanceMin = 0;
        if (distance == null) this.distanceMax = Integer.MAX_VALUE;
        else this.distanceMax = distance;
        search_type = defaultSearchAlgorithm;
    }

    /**
     *
     * This method will construct a traverser that followa the route indicated
     * by property, for up to the specified distnace. The direction list will
     * indicated wich direction is admitted.
     * The admited direction will be outgoing. The default search algorithm
     * search will be used
     *
     * @param property The route of properties to follow
     * @param directions The direction of the relationship
     * @param distance The maximum search distance
     */
    public MapStoreTraverserDescriptor(Set<Integer> nodes,List<String> property, List<Integer> directions, Integer distance) {
        initialNodes = nodes;
        route = new ArrayList<String>(property);
        directions = new ArrayList<Integer>(directions);
        if (directions.size() != route.size()) throw new IllegalArgumentException("The route and direction list must have the same length");
        this.distanceMin = 0;
        if (distance == null) this.distanceMax = Integer.MAX_VALUE;
        else this.distanceMax = distance;
        search_type = defaultSearchAlgorithm;
    }

    /**
     *
     * This method will construct a traverser that follows the route indicated
     * by property, for a given range. The direction list will indicated wich
     * direction is admitted for each step. The default search algorithm search
     * will be used
     *
     * @param property The route of properties to follow
     * @param directions The direction of the relationship
     * @param distanceMin The minimum search distance. Can not be null
     * @param distanceMax The maximum search distance
     */
    public MapStoreTraverserDescriptor(Set<Integer> nodes,List<String> property, List<Integer> directions, Integer distanceMin, Integer distanceMax) {
        initialNodes = nodes;
        if (distanceMin == null) throw new IllegalArgumentException("The minimum distance can not be null");
        else this.distanceMin = distanceMin;
        route = new ArrayList<String>(property);
        directions = new ArrayList<Integer>(directions);
        if (directions.size() != route.size()) throw new IllegalArgumentException("The route and direction list must have the same length");
        if (distanceMax == null) this.distanceMax = Integer.MAX_VALUE;
        else this.distanceMax = distanceMax;
        search_type = defaultSearchAlgorithm;
    }

    /**
     *
     * This method will construct a traverser that follows the route indicated
     * by property, for a given range. The direction list will
     * indicated wich direction is admitted for each step.
     *
     * @param property The route of properties to follow
     * @param directions The direction of the relationship
     * @param distanceMin The minimum search distance. Can not be null
     * @param distanceMax The maximum search distance
     * @param search The search algorithm to use
     */
    public MapStoreTraverserDescriptor(Set<Integer> nodes,List<String> property, List<Integer> directions, Integer distanceMin, Integer distanceMax, int search) {
        initialNodes = nodes;
        if (distanceMin == null) throw new IllegalArgumentException("The minimum distance can not be null");
        else this.distanceMin = distanceMin;
        route = new ArrayList<String>(property);
        directions = new ArrayList<Integer>(directions);
        if (directions.size() != route.size()) throw new IllegalArgumentException("The route and direction list must have the same length");
        if (distanceMax == null) this.distanceMax = Integer.MAX_VALUE;
        else this.distanceMax = distanceMax;
        search_type = defaultSearchAlgorithm;
    }



    public String getRouteForDistance(int distance) {
        if (distanceMax < distance) throw new IndexOutOfBoundsException("The traverser is limited to " + distanceMax + "hops");
        int mod = distance%route.size();
        return route.get(mod);
    }

    public int getDirectionForDistance(int distance) {
        if (distanceMax < distance) throw new IndexOutOfBoundsException("The traverser is limited to " + distanceMax + "hops");
        int mod = distance%directions.size();
        return directions.get(mod);
    }

    public int getSearchAlgorithm() {
        return search_type;
    }

    public Integer getDistanceMax() {
        return distanceMax;
    }

    public Integer getDistanceMin() {
        return distanceMin;
    }

    public Set<Integer> getInitialNodes() {
        return initialNodes;
    }


}
