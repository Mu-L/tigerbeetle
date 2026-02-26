const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const page_allocator_vtable = std.heap.page_allocator.vtable;

/// Like `std.heap.page_allocator`, but on Linux applies `MADV_HUGEPAGE` to
/// allocated regions so that the kernel may back them with 2 MiB transparent
/// huge pages, reducing TLB pressure for large allocations.
///
/// Only `alloc` is intercepted. `resize` and `remap` inherit the VMA flags
/// (including `VM_HUGEPAGE`) set on the original mapping, so they need no
/// additional `madvise` call.
///
/// On non-Linux targets this is identical to `std.heap.page_allocator`.
pub const huge_page_allocator: Allocator = .{
    .ptr = undefined,
    .vtable = if (builtin.target.os.tag == .linux) &vtable else page_allocator_vtable,
};

const vtable: Allocator.VTable = .{
    .alloc = alloc,
    .resize = page_allocator_vtable.resize,
    .remap = page_allocator_vtable.remap,
    .free = page_allocator_vtable.free,
};

fn alloc(context: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
    const ptr = page_allocator_vtable.alloc(context, n, alignment, ra) orelse return null;
    // This is just a hint, so if it fails we can safely ignore it.
    std.posix.madvise(@alignCast(ptr), n, std.posix.MADV.HUGEPAGE) catch {};
    return ptr;
}

const testing = std.testing;

test "huge_page_allocator: basic alloc and free" {
    const slice = try huge_page_allocator.alloc(u8, 4096);
    defer huge_page_allocator.free(slice);

    @memset(slice, 0xab);
    try testing.expectEqual(@as(u8, 0xab), slice[0]);
}

test "huge_page_allocator: large THP-eligible allocation" {
    // 4 MiB — large enough for THP promotion on Linux.
    const size = 4 * 1024 * 1024;
    const slice = try huge_page_allocator.alloc(u8, size);
    defer huge_page_allocator.free(slice);

    @memset(slice, 0xcd);
    try testing.expectEqual(@as(u8, 0xcd), slice[size - 1]);
}

test "huge_page_allocator: as ArenaAllocator backing" {
    var arena = std.heap.ArenaAllocator.init(huge_page_allocator);
    defer arena.deinit();

    const alloc1 = try arena.allocator().alloc(u8, 1024);
    const alloc2 = try arena.allocator().alloc(u8, 2048);
    @memset(alloc1, 1);
    @memset(alloc2, 2);
    try testing.expectEqual(@as(u8, 1), alloc1[0]);
    try testing.expectEqual(@as(u8, 2), alloc2[0]);
}
