/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.gondola.container;

import com.yahoo.gondola.Config;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class AdminCli {

    AdminClient adminClient;


    public static void main(String args[])
        throws Exception {
        new AdminCli(args);
    }

    public AdminCli(String[] args) throws Exception {
        Config config = ConfigLoader.getConfigInstance(URI.create("classpath:///gondola.conf"));
        adminClient = AdminClient.getInstance(config, "adminCli");
        if (args.length == 1) {
            usage();
        }

        switch (args[1]) {
            case "enable":
            case "disable":
                break;
            case "mergeShard":
            case "splitShard":
                break;
            case "setConfig":
                URI uri = URI.create(args[2]);
                Config configInstance = ConfigLoader.getConfigInstance(uri);
                adminClient.setConfig(configInstance.getFile());
                break;
            case "getConfig":
                System.out.println(Files.readAllLines(Paths.get(adminClient.getConfig().getFile().getAbsolutePath())));
                break;
            case "enableTracing":
            case "disableTracing":
                break;
            case "getStates":
                printStates(adminClient.getServiceStatus());
                break;
            case "setBuckets":
                if (args.length != 6) {
                    usage();
                }
                int rangeStart = Integer.parseInt(args[2]);
                int rangeEnd = Integer.parseInt(args[3]);
                String fromShard = args[4];
                String toShard = args[5];
                adminClient.assignBuckets(rangeStart, rangeEnd, fromShard, toShard);
                break;
            case "inspectUri":
                if (args.length != 3) {
                    usage();
                }
                adminClient.inspectRequestUri(config.getHostIds().get(0), args[2]);
                break;
            case "setLeader":
                if (args.length != 3) {
                    usage();
                }
                String hostId = args[2];
                for (String shardId  : config.getShardIds(hostId)) {
                    adminClient.setLeader(hostId, shardId);
                }
                break;
            default:
                usage();
        }
    }

    private void printStates(Map<String, Object> serviceStatus) {
        for (Map.Entry<String, Object> e : serviceStatus.entrySet()) {
            String key = e.getKey();
            Object value = e.getValue();
            System.out.println(key);
            if (value instanceof Map) {
                Map v = (Map) value;
                System.out.println("  " + v.get("gondolaStatus"));
                System.out.println("  " + v.get("bucketTable"));
                System.out.println("  " + v.get("actions"));
            } else {
                System.out.println("  " + value);
            }
        }
    }

    private void usage() {
        String usage = "Gondola admin client\n\n" +
                       "usage: adminCli -c <config> <command> [<args>]\n\n" +
                       "These are common commands used in various situation: \n\n" +
                       "Config related: \n" +
                       "   enable         <hostId>\n" +
                       "   disable        <hostId>\n" +
                       "   enableTracing  <hostId>\n" +
                       "   disableTracing <hostId>\n" +
                       "   setLeader      <hostId>\n" +
                       "   mergeShard     <fromShardId> <toShardId>\n" +
                       "   splitShard     <fromShardId> <toShardId>\n" +
                       "   setBuckets     <bucketStart> <bucketEnd> <fromShardId> <toShardId>\n" +
                       "   inspectUri     <URI>\n" +
                       "   status         \n" +
                       "   getConfig      \n" +
                       "   setConfig      \n" +
                       "   getHostIds     \n";
        System.out.println(usage);
        System.exit(1);
    }
}
