/*
 * Copyright 2015, Yahoo Inc.
 * Copyrights licensed under the New BSD License.
 * See the accompanying LICENSE file for terms.
 */

function loadDoc() {
    var xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function () {
        function print_metrics(timer, meters) {
            var s = "";
            for (key in timer) {
                s +=
                    key + ": " + Math.floor(timer[key]['oneMinuteRate'] * 100) / 100 + " rps, "
                    + Math.floor(timer[key]['meanResponseTime'] * 100) / 100 + " ms/req<br>";
            }
            for (key in meters) {
                s += key + ": " + timer[key]['oneMinuteRate'] + " rps<br>";
            }
            return s;
        }

        function print_bucket_table(bucketTable) {
            var s = "";
            for (key in bucketTable) {
                s += key + ": " + print_shardId(bucketTable[key]['shardId']);
                if (bucketTable[key]['migratingShardId']) {
                    s += " ==> " + print_shardId(bucketTable[key]['migratingShardId']);
                }
            }
            return s;
        }

        function print_shardId(shardId) {
            var color = "";
            if (shardId == "shard1") {
                color = "blue";
            } else {
                color = "brown";
            }
            return "<font color='" + color + "'>" + shardId + "</font>"
        }

        function print_shard_manager(shardManager) {
            var s = "";
            if (shardManager && shardManager['shardManagerServer']
                && shardManager['shardManagerServer']['currentStats']) {
                for (var memberId in shardManager['shardManagerServer']['currentStats']) {
                    s += print(shardManager['shardManagerServer']['currentStats'][memberId]) + "<br>";
                }
            }
            return s;
        }

        function print_actions(actions) {
            var s = "";
            for (var memberId in actions) {
                s += print(actions[memberId]) + "<br>";
            }
            return s;
        }

        function print_shard_ids(shardIds) {
            var s = "";
            shardIds.forEach(function (e) {
                s += print_shardId(e) + "<br>";
            });
            return s;
        }

        var t = {};
        t['columns'] = [
            {
                field: 'hostId',
                title: 'Host ID'
            },
            {
                field: 'shardId',
                title: 'Shard ID'
            },
            {
                field: 'status',
                title: 'status'
            },
            {
                field: 'metrics',
                title: 'metrics'
            },
            {
                field: 'bucketTable',
                title: 'Bucket table'
            },
            {
                field: 'shardManagerStatus',
                title: 'shard manager'
            },
            {
                field: 'action',
                title: 'action'
            },
            {
                field: 'pid',
                title: 'pid'
            },
            {
                field: 'enable',
                title: 'enable/disable'
            }
        ];
        function print_status(status) {
            var s = "";

            function print_enable(enable) {
                if (enable == true) {
                    return "<font color=blue>" + enable + "</font>";
                }
                return "<font color=red>" + enable + "</font>";
            }

            function print_role(role) {
                if (role == "LEADER") {
                    return "<b>" + role + "</b>";
                }
                return role;
            }

            for (var shardId in status) {
                s += "commitIndex=" + status[shardId]['commitIndex'] + "<br>";
                s += "appliedIndex=" + status[shardId]['appliedIndex'] + "<br>";
                s += "savedIndex=" + status[shardId]['savedIndex'] + "<br>";
                s += "enabled=" + print_enable(status[shardId]['enabled']) + "<br>";
                s += "role=" + print_role(status[shardId]['role']) + "<br>";
            }
            return s;
        }

        if (xhttp.readyState == 4 && xhttp.status == 200) {
            jsonObject = JSON.parse(xhttp.responseText);

            t['data'] = [];
            var hosts = Object.keys(jsonObject).sort();
            for (var i in hosts) {
                var host = hosts[i];
                var o = jsonObject[host];
                var shardId = "";
                var status = "";
                var metrics = "";
                var bucketTable = "";
                var shardManager = "";
                var action = "";
                var pid = "";
                var enable = "";
                if (o != null) {
                    var shardIds = Object.keys(o['gondolaStatus']);
                    shardId = print_shard_ids(shardIds);
                    status = print_status(o['gondolaStatus']);
                    metrics = print_metrics(o['timers'], o['meters']);
                    bucketTable = print_bucket_table(o['bucketTable']);
                    pid = o['pid'];
                    if (o['shardManager']) {
                        shardManager = print_shard_manager(o['shardManager']);
                        if (o['shardManager']['shardManagerServer']) {
                            action = print_actions(o['shardManager']['shardManagerServer']['actions']);
                        }
                    }
                    enable =
                        "<button onclick='enable(\"" + host
                        + "\", \"true\");'>enable</button> <button onclick='enable(\"" + host
                        + "\", \"false\");'>disable</button>";
                } else {
                    status = "<font color=red>DOWN</font>";
                }

                var d = {
                    id: host,
                    hostId: host,
                    shardId: shardId,
                    status: status,
                    metrics: metrics,
                    bucketTable: bucketTable,
                    shardManagerStatus: shardManager,
                    action: action,
                    pid: pid,
                    enable: enable
                };
                t['data'].push(d);
            }
        }
        $('#table').bootstrapTable(t);
        $('#table').bootstrapTable('load', t);
        document.getElementById("updateTime").innerText = "Last update: " + new Date();
    };
    xhttp.open("GET", "/api/gondola/v1/serviceStatus", true);
    xhttp.send();
}

function print(obj, maxDepth, prefix) {
    var result = '';
    if (!prefix) {
        prefix = '';
    }
    for (var key in obj) {
        if (typeof obj[key] == 'object') {
            if (maxDepth !== undefined && maxDepth <= 1) {
                result += (prefix + key + '=object [max depth reached]<br>');
            } else {
                result += print(obj[key], (maxDepth) ? maxDepth - 1 : maxDepth, prefix + key + '.');
            }
        } else {
            result += (prefix + key + '=' + obj[key] + '<br>');
        }
    }
    return result;
}

loadDoc();

setInterval(function () {
    loadDoc();
}, 500);

function enable(host, enable) {
    var xhr = new XMLHttpRequest();
    xhr.open("POST", "/api/gondola/v1/enableHost?hostId=" + host + "&enable=" + enable, false);
    xhr.send(" ");
}
