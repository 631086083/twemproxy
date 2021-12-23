from .common import *
from redisbloom.client import Client


def getconn():
    for r in all_redis:
        c = Client(r.host(), r.port())
        c.flushdb()

    r = Client(nc.host(), nc.port())
    return r


def load_bf_module():
    for r in all_redis:
        c = Client(r.host(), r.port())
        modules = c.execute_command("module", "list")
        module_names = [m[1] for m in modules]
        if "bf" not in module_names and b'bf' not in module_names:
            return False
    return True


def test_bf_addexists():
    if not load_bf_module():
        return
    r = getconn()
    add = r.bfAdd("key", "1")
    assert_equal(1, add)
    assert_equal(1, r.bfExists("key", "1"))


def test_bf_maddmexists():
    if not load_bf_module():
        return
    r = getconn()
    add = r.bfMAdd("key", "a", "b", "c")
    assert_equal([1, 1, 1], add)
    assert_equal([1, 1, 1], r.bfMExists("key", "a", "b", "c"))


def test_bf_reserve():
    if not load_bf_module():
        return
    r = getconn()
    bf_create = r.bfCreate("bfc", 0.001, 500, None, None)
    assert_equal(True, bf_create)

    bfc_info = r.bfInfo("bfc")
    assert_equal(1144, bfc_info.size)
    assert_equal(500, bfc_info.capacity)
    assert_equal(2, bfc_info.expansionRate)
    assert_equal(0, bfc_info.insertedNum)
    assert_equal(1, bfc_info.filterNum)


def test_bf_insert():
    if not load_bf_module():
        return
    r = getconn()
    bf_insert = r.bfInsert("bfi", [1, 2, 3, 4], 500, 0.001, None, None, None)
    assert_equal([1, 1, 1, 1], bf_insert)

    bfi_info = r.bfInfo("bfi")
    assert_equal(1144, bfi_info.size)
    assert_equal(500, bfi_info.capacity)
    assert_equal(2, bfi_info.expansionRate)
    assert_equal(4, bfi_info.insertedNum)
    assert_equal(1, bfi_info.filterNum)


def test_bf_scanload():
    if not load_bf_module():
        return
    r = getconn()
    bf_insert = r.bfInsert("bfscan", [1, 2, 3, 4], 500, 0.001, None, None, None)

    chunks = []
    iter = 0
    while True:
        iter, data = r.bfScandump("bfscan", iter)
        if iter == 0:
            break
        else:
            chunks.append([iter, data])

    # Load it back
    for chunk in chunks:
        iter, data = chunk
        r.bfLoadChunk("bfload", iter, data)
    bfload_info = r.bfInfo("bfload")
    assert_equal(1144, bfload_info.size)
    assert_equal(500, bfload_info.capacity)
    assert_equal(0, bfload_info.expansionRate)
    assert_equal(4, bfload_info.insertedNum)
    assert_equal(1, bfload_info.filterNum)
