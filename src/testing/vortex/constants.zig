const std = @import("std");
const constants = @import("../../constants.zig");

pub usingnamespace constants;

pub const cluster_id = 1;
pub const replica_count = 3;
pub const connections_count_max = @divFloor(constants.clients_max, replica_count);

// We allow the cluster to not make progress processing requests for this amount of time. After
// that it's considered a test failure.
pub const liveness_requirement_seconds = 120;
pub const liveness_requirement_micros = liveness_requirement_seconds * std.time.us_per_s;

// How many replicas can be faulty while still expecting the cluster to make progress (based on
// 2f+1).
pub const liveness_faulty_replicas_max = @divFloor(replica_count - 1, 2);

pub const replica_ports_actual = brk: {
    var ports: [replica_count]u16 = undefined;
    var replica_num: u16 = 0;
    while (replica_num < replica_count) : (replica_num += 1) {
        ports[replica_num] = 4000 + replica_num;
    }
    break :brk ports;
};

pub const replica_ports_proxied = brk: {
    var ports: [replica_count]u16 = undefined;
    var replica_num: u16 = 0;
    while (replica_num < replica_count) : (replica_num += 1) {
        ports[replica_num] = 3000 + replica_num;
    }
    break :brk ports;
};
