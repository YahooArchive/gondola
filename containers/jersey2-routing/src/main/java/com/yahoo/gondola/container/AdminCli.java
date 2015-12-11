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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AdminCli {

    AdminClient adminClient;


    public static void main(String args[])
        throws Exception {
        new AdminCli(args);
    }

    public AdminCli(String[] stringArgs) throws Exception {
        List<String> argsList = Arrays.asList(stringArgs);
        if (argsList.size() < 2) {
            usage();
        }

        String confFile = "file:///examples/kv-server/src/main/resources/gondola.conf";
        for (int i = 0; i < argsList.size(); i++) {
            if (argsList.get(i).equals("-c")) {
                confFile = argsList.get(i+1);
                argsList.remove(i);
                argsList.remove(i);
                break;
            }
        }
        Config config = ConfigLoader.getConfigInstance(URI.create(confFile));
        adminClient = AdminClient.getInstance(config, "adminCli");
        if (argsList.size() == 0) {
            usage();
        }

        switch (argsList.get(0)) {
            case "enable":
            case "disable":
                break;
            case "mergeShard":
            case "splitShard":
                break;
            case "setConfig":
                URI uri = URI.create(argsList.get(1));
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
                if (argsList.size() != 5) {
                    usage();
                }
                int rangeStart = Integer.parseInt(argsList.get(1));
                int rangeEnd = Integer.parseInt(argsList.get(2));
                String fromShard = argsList.get(3);
                String toShard = argsList.get(4);
                adminClient.assignBuckets(rangeStart, rangeEnd, fromShard, toShard);
                break;
            case "inspectUri":
                if (argsList.size() != 2) {
                    usage();
                }
                adminClient.inspectRequestUri(config.getHostIds().get(0), argsList.get(1));
                break;
            case "setLeader":
                if (argsList.size() != 2) {
                    usage();
                }
                String hostId = argsList.get(1);
                for (String shardId : config.getShardIds(hostId)) {
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
                       "   setSlave       <fromShard> <toShard>\n" +
                       "   unsetSlave     <fromShard> <toShard>\n" +
                       "   mergeShard     <fromShardId> <toShardId>\n" +
                       "   splitShard     <fromShardId> <toShardId>\n" +
                       "   setBuckets     <bucketStart> <bucketEnd> <fromShardId> <toShardId>\n" +
                       "   inspectUri     <uri>\n" +
                       "   status         \n" +
                       "   getConfig      \n" +
                       "   setConfig      <file>\n" +
                       "   getHostIds     \n";
        System.out.println(usage);
        System.exit(1);
    }
}
