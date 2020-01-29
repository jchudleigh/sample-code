/*
 * ClusteringThread.java
 * 
 * Author: Jeremiah Chudleigh
 * Date: 2020-01-29
 *
 * This thread, given a blocking event queue and user lookup information, 
 * will load a collection of 3D points (or 2D and one dimension of time) from 
 * the database, and cluster them into centroids using the K-Means clustering
 * algorithm provided in the Apache Commons Machine Learning library.
 *
 */
package ca.blueocto.blueoctobot;

import java.util.concurrent.BlockingQueue;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.clustering.MultiKMeansPlusPlusClusterer;

/**
 *
 * @author jchudleigh
 */
public class ClusteringThread implements Runnable {

    private static final int NEW_RESULT = 0;
    private static final int SUCCESS = 0;
    private static final int ADMIN_TEST_USER = 2;
    private static final int SINGLE_CLUSTER = 1;
    private static final int FOUR_RESULTS = 4;
    private static final String DEFAULT_NAME = "default";
    private static final int TRIAL_RUNS = 1000;
    private static final int MAX_ITERATIONS = 4000;
    private final BlockingQueue<Event> eventQueue;
    private int userID;
    private int kValue;
    private String groupName;
    private String channel;

    public ClusteringThread(BlockingQueue<Event> events) {
        this(events, ADMIN_TEST_USER, SINGLE_CLUSTER, DEFAULT_NAME, "");
    }

    public ClusteringThread(BlockingQueue<Event> events,
            Integer userID, Integer kValue, String groupName, String channel) {
        this.eventQueue = events;
        this.userID = userID;
        this.kValue = kValue;
        this.groupName = groupName;
        this.channel = channel;
    }

    @Override
    public void run() {
        // get coords from db
        List<CoordinateBean> beans = new LinkedList<>();
        String whereClause = "user_id = " + userID + " AND isIncluded = 1 "
                + "AND group_name = '" + groupName + "'";
        CoordinateBean.loadBeansByClause(whereClause, beans);
        // cluster points
        List<ClusterPoint> points = new LinkedList<>();
        beans.forEach((bean) -> {
            points.add(bean.getClusterPoint());
        });
        if (kValue > points.size()) {
            // looking for more centroids then there are points
            Event errorEvent = new Event(Event.Type.RESPONSE,
                    new String[]{"Not enough coordinates to cluster that "
                        + "many centroids"});
            errorEvent.setChannel(channel);
            eventQueue.add(errorEvent);
        } else {
            int clusters = kValue;
            KMeansPlusPlusClusterer<ClusterPoint> kmpp
                    = new KMeansPlusPlusClusterer(clusters, MAX_ITERATIONS);
            MultiKMeansPlusPlusClusterer<ClusterPoint> wrapper
                    = new MultiKMeansPlusPlusClusterer<>(kmpp, TRIAL_RUNS);
            List<CentroidCluster<ClusterPoint>> centroids 
                    = wrapper.cluster(points);
            List<String> result;
            result = new LinkedList<>();
            centroids.forEach((CentroidCluster<ClusterPoint> centroid) -> {
                // reformat string output from center object
                StringBuilder centroidText = new StringBuilder();
                double[] point = centroid.getCenter().getPoint();
                centroidText.append(CoordinateBean.SEPARATOR);
                for (int index = 0; index < point.length; index++) {
                    centroidText.append(String.format("%.10f", point[index]));
                    centroidText.append(CoordinateBean.SEPARATOR);
                }
                result.add(centroidText.toString());
            });
            LinkedList<String> responseList = new LinkedList();
            // put resulting centroids in db
            ClusterResultBean resultBean = new ClusterResultBean(userID,
                    NEW_RESULT, groupName, clusters, points.size(),
                    TRIAL_RUNS, MAX_ITERATIONS);
            int storageResult = ClusterResultBean.storeBean(resultBean);
            if (storageResult == SUCCESS) {
                Integer newResultID
                        = ClusterResultBean.getLatestResultIDByUserID(
                                userID);
                // for each result cluster, set result id, store it
                for (int index = 0; index < result.size(); index++) {
                    String current = result.get(index);
                    System.out.println(current);
                    StringBuilder builder = new StringBuilder();
                    builder.append("GPS:")
                            .append("R_").append(newResultID)
                            .append("_Cluster_").append(Integer.toString(index))
                            .append(current);
                    responseList.add(builder.toString());
                    CoordinateBean bean = new CoordinateBean(
                            userID, builder.toString());
                    bean.setIsIncluded(false);
                    bean.setResultID((newResultID));
                    bean.setGroupName(groupName);
                    CoordinateBean.storeBean(bean);
                }
                // Only return results in groups of four at a time
                do {
                    String[] fourResults = new String[FOUR_RESULTS];
                    for (int index = 0; index < fourResults.length; index++){
                        if (!responseList.isEmpty()) {
                            fourResults[index] = responseList.removeFirst();
                        } else {
                            fourResults[index] = "";
                        }
                    }
                    Event displayClusters = new Event(Event.Type.RESPONSE, 
                            fourResults);
                    displayClusters.setChannel(channel);
                    eventQueue.add(displayClusters);
                } while (!responseList.isEmpty());
            } else {
                Event errorEvent = new Event(Event.Type.RESPONSE,
                        new String[]{
                            "An error occured while clustering, check logs"});
                errorEvent.setChannel(channel);
                eventQueue.add(errorEvent);
            }
        }
    }
}
