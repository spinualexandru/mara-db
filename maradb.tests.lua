local maradb = require('maradb')

-- Utility functions
local function assert_equals(expected, actual, message)
    if expected ~= actual then
        error(message .. " (Expected " .. tostring(expected) .. ", got " .. tostring(actual) .. ")")
    end
end

local function assert_true(condition, message)
    if not condition then
        error(message)
    end
end

local function assert_error(func, expected_error)
    local success, err = pcall(func)
    if success then
        error("Expected error but operation succeeded")
    end
    return err
end

-- Clean up test database
local function cleanup()
    os.remove("test_db.db")
end

-- Test basic database operations
local function test_database_creation()
    cleanup()

    local db = maradb.open("test_db")
    assert_true(db ~= nil, "Database should be created")

    local result = db:close()
    assert_true(result, "Database should close successfully")

    local db2 = maradb.open("test_db")
    assert_true(db2 ~= nil, "Database should be reopened")
    db2:close()
end

-- Test collection operations
local function test_collection_operations()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    assert_true(users ~= nil, "Collection should be created")
    assert_equals("users", users.name, "Collection name should match")
    assert_equals(0, #users.data, "New collection should be empty")

    db:close()

    local db2 = maradb.open("test_db")
    local users2 = db2:collection("users")
    assert_true(users2 ~= nil, "Collection should persist after reopen")
    db2:close()
end

-- Test basic CRUD operations
local function test_basic_crud()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    -- Insert
    local id = users:insert({ name = "Alice", age = 30 })
    assert_true(id > 0, "Insert should return valid ID")
    assert_equals(1, #users.data, "Collection should have one item")

    -- Query
    local results = users:where({ name = "Alice" })
    assert_equals(1, #results, "Query should find one result")
    assert_equals("Alice", results[1].name, "Query result should match inserted data")
    assert_equals(30, results[1].age, "Query result should match inserted data")

    -- Update
    users:update({ name = "Alice" }, { age = 31 })
    results = users:where({ name = "Alice" })
    assert_equals(31, results[1].age, "Update should modify data")

    -- Remove
    users:remove({ name = "Alice" })
    results = users:where({ name = "Alice" })
    assert_equals(0, #results, "Remove should delete data")

    db:close()
end

-- Test index creation and enforcement
local function test_index_creation()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    users:createIndex("username")
    users:insert({ username = "alice", age = 30 })

    local err = assert_error(function()
        users:insert({ age = 25 })
    end)
    assert_true(err:match("Missing indexed field"), "Should error on missing index field")

    db:close()

    -- Reopen and verify index is still enforced
    local db2 = maradb.open("test_db")
    local users2 = db2:collection("users")

    local err2 = assert_error(function()
        users2:insert({ age = 26 })
    end)
    assert_true(err2:match("Missing indexed field"), "Index should persist across sessions")

    users2:insert({ username = "bob", age = 40 })
    db2:close()
end

-- Test uniqueness constraint
local function test_uniqueness_constraint()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    users:createIndex("email")
    users:insert({ email = "alice@example.com", name = "Alice" })

    local err = assert_error(function()
        users:insert({ email = "alice@example.com", name = "Alice2" })
    end)
    assert_true(err:match("Duplicate value"), "Should error on duplicate value")

    db:close()

    -- Reopen and verify uniqueness is still enforced
    local db2 = maradb.open("test_db")
    local users2 = db2:collection("users")

    local err2 = assert_error(function()
        users2:insert({ email = "alice@example.com", name = "Alice3" })
    end)
    assert_true(err2:match("Duplicate value"), "Uniqueness should persist across sessions")

    users2:insert({ email = "bob@example.com", name = "Bob" })
    db2:close()
end

-- Test string insert with indexes
local function test_string_insert_with_index()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    users:createIndex("name")
    users:insert("Alice")

    local results = users:where({ name = "Alice" })
    assert_equals(1, #results, "String value should be inserted correctly")

    local err = assert_error(function()
        users:insert("Alice")
    end)
    assert_true(err:match("Duplicate value"), "Should prevent duplicate strings")

    db:close()
end

-- Test multiple collections with indexes
local function test_multiple_collection_indexes()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")
    local posts = db:collection("posts")

    users:createIndex("email")
    posts:createIndex("slug")

    users:insert({ email = "alice@example.com", name = "Alice" })
    posts:insert({ slug = "first-post", title = "First Post" })

    local err1 = assert_error(function()
        users:insert({ email = "alice@example.com", name = "Duplicate" })
    end)

    local err2 = assert_error(function()
        posts:insert({ slug = "first-post", title = "Duplicate" })
    end)

    db:close()

    -- Reopen and check indexes still work
    local db2 = maradb.open("test_db")
    local users2 = db2:collection("users")
    local posts2 = db2:collection("posts")

    local err3 = assert_error(function()
        users2:insert({ email = "alice@example.com" })
    end)

    local err4 = assert_error(function()
        posts2:insert({ slug = "first-post" })
    end)

    db2:close()
end

local function test_nested_objects()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    -- Insert document with nested object
    users:insert({
        name = "Bob",
        profile = {
            age = 30,
            address = {
                city = "New York",
                zip = "10001"
            }
        }
    })

    db:close()

    -- Reopen and verify nested data persists
    local db2 = maradb.open("test_db")
    local users2 = db2:collection("users")
    local results = users2:where({ name = "Bob" })

    assert_equals(1, #results, "Should find user with nested data")
    assert_equals(30, results[1].profile.age, "Nested object field should be preserved")
    assert_equals("New York", results[1].profile.address.city, "Deeply nested field should be preserved")

    db2:close()
end

local function test_nested_arrays()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    -- Insert document with nested arrays
    users:insert({
        username = "charlie",
        tags = { "programmer", "gamer" },
        posts = {
            { title = "First post", likes = 10 },
            { title = "Second post", likes = 15 }
        }
    })

    db:close()

    -- Reopen and verify nested arrays persist
    local db2 = maradb.open("test_db")
    local users2 = db2:collection("users")
    local results = users2:where({ username = "charlie" })

    assert_equals(1, #results, "Should find user with nested arrays")
    assert_equals(2, #results[1].tags, "Array should have correct length")
    assert_equals("programmer", results[1].tags[1], "Array elements should be preserved")
    assert_equals(2, #results[1].posts, "Nested array should have correct length")
    assert_equals(15, results[1].posts[2].likes, "Objects in arrays should be preserved")

    db2:close()
end

local function test_complex_nesting()
    cleanup()

    local db = maradb.open("test_db")
    local inventory = db:collection("inventory")

    -- Insert document with complex nesting
    inventory:createIndex("sku")
    inventory:insert({
        sku = "ABC123",
        name = "Widget",
        categories = { "tools", "home" },
        variants = {
            {
                color = "blue",
                sizes = { "S", "M", "L" },
                prices = {
                    regular = 19.99,
                    sale = {
                        value = 14.99,
                        untils = "2023-12-31"
                    }
                }
            },
            {
                color = "red",
                sizes = { "M", "L" },
                prices = {
                    regular = 21.99,
                    sale = {
                        value = 16.99,
                        untils = "2023-12-31"
                    }
                }
            }
        }
    })

    local results = inventory:where({ sku = "ABC123" })
    assert_equals("blue", results[1].variants[1].color, "Complex nested data should be accessible")
    assert_equals(14.99, results[1].variants[1].prices.sale.value, "Deeply nested values should be accessible")

    db:close()

    -- Reopen and verify complex nested data persists
    local db2 = maradb.open("test_db")
    local inventory2 = db2:collection("inventory")
    local results2 = inventory2:where({ sku = "ABC123" })

    assert_equals(2, #results2[1].variants, "Nested array should maintain count")
    assert_equals("2023-12-31", results2[1].variants[2].prices.sale.untils, "Deeply nested values should persist")

    db2:close()
end

-- Test nil values handling
local function test_nil_values()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    -- Insert with nil values
    users:insert({
        username = "dave",
        email = nil,
        active = true
    })

    local results = users:where({ username = "dave" })
    assert_equals(1, #results, "Should store document with nil values")
    assert_true(results[1].email == nil, "Should preserve nil values")

    -- Querying for nil values
    local nil_results = users:where({ email = nil })
    assert_equals(1, #nil_results, "Should be able to query for nil values")

    db:close()

    -- Check persistence of nil values
    local db2 = maradb.open("test_db")
    local users2 = db2:collection("users")
    local results2 = users2:where({ username = "dave" })
    assert_true(results2[1].email == nil, "Nil values should persist across sessions")

    db2:close()
end

-- Test nil values in nested structures
local function test_nested_nil_values()
    cleanup()

    local db = maradb.open("test_db")
    local products = db:collection("products")

    -- Insert document with nil values at different nesting levels
    products:insert({
        code = "PROD-123",
        name = "Test Product",
        details = {
            color = "blue",
            weight = nil,
            dimensions = {
                width = 10,
                height = 20,
                depth = nil
            }
        },
        categories = { "electronics", nil, "sale" },
        related_codes = nil
    })

    db:close()

    -- Verify nil values in nested structures
    local db2 = maradb.open("test_db")
    local products2 = db2:collection("products")
    local results = products2:where({ code = "PROD-123" })

    assert_equals(1, #results, "Should find document with nested nil values")
    assert_true(results[1].details.weight == nil, "Shallow nested nil should be preserved")
    assert_true(results[1].details.dimensions.depth == nil, "Deep nested nil should be preserved")
    assert_true(results[1].categories[2] == nil, "Nil in array should be preserved")
    assert_true(results[1].related_codes == nil, "Top-level nil should be preserved")

    db2:close()
end

-- Test indexed fields with nil values
local function test_indexed_nil_values()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    users:createIndex("email")

    -- This should fail since the indexed field is missing
    local err = assert_error(function()
        users:insert({
            username = "bob",
            active = true
            -- email is missing
        })
    end)
    assert_true(err:match("Missing indexed field"), "Should reject document missing indexed field")

    -- This should also fail with explicit nil
    local err2 = assert_error(function()
        users:insert({
            username = "bob",
            email = nil,
            active = true
        })
    end)
    assert_true(err2:match("Missing indexed field"), "Should reject document with nil indexed field")

    db:close()
end

-- Test where method with partial updates
local function test_where_with_partial_update()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    -- Insert test data
    users:insert({ name = "Alex", level = 5, status = "active" })
    users:insert({ name = "Bob", level = 10, status = "active" })
    users:insert({ name = "Charlie", level = 15, status = "inactive" })

    -- Perform partial update with where
    local results = users:where({ status = "active" }, { level = 20 })

    -- Verify results count
    assert_equals(2, #results, "Should find and return 2 active users")

    -- Check that partial update was applied
    local alex = users:where({ name = "Alex" })[1]
    local bob = users:where({ name = "Bob" })[1]

    assert_equals(20, alex.level, "Alex's level should be updated")
    assert_equals(20, bob.level, "Bob's level should be updated")
    assert_equals("active", alex.status, "Status should remain unchanged")

    -- Verify non-matching documents weren't updated
    local charlie = users:where({ name = "Charlie" })[1]
    assert_equals(15, charlie.level, "Non-matching documents should not be updated")

    db:close()
end

-- Test get method
local function test_get_method()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    -- Insert test data
    users:insert({ name = "Alice", age = 30 })
    users:insert({ name = "Bob", age = 25 })

    -- Test get with match
    local alice = users:get({ name = "Alice" })
    assert_true(alice ~= nil, "Should find Alice")
    assert_equals("Alice", alice.name, "Should return correct record")
    assert_equals(30, alice.age, "Should include all fields")

    -- Test get with no match
    local nobody = users:get({ name = "Nobody" })
    assert_true(nobody == nil, "Should return nil for no match")

    -- Test get preserves references
    local bob = users:get({ name = "Bob" })
    bob.age = 26  -- Modify the returned object

    -- Verify the change persisted to collection
    local bob_check = users:get({ name = "Bob" })
    assert_equals(26, bob_check.age, "Modifications to returned objects should affect the collection")

    db:close()
end

-- Test upsert method
local function test_upsert_method()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    -- Insert initial data
    users:insert({ name = "Dave", level = 5, status = "active" })

    -- Test update case
    local updated = users:upsert({ name = "Dave" }, { level = 10, title = "Admin" })
    assert_equals(1, #updated, "Should return 1 updated record")
    assert_equals(10, updated[1].level, "Should update level field")
    assert_equals("Admin", updated[1].title, "Should add new field")
    assert_equals("active", updated[1].status, "Should preserve existing fields")

    -- Verify update affected the collection
    local dave = users:get({ name = "Dave" })
    assert_equals(10, dave.level, "Update should persist in collection")

    -- Test insert case
    local inserted = users:upsert({ name = "Eve" }, { level = 7, status = "new" })
    assert_equals(1, #inserted, "Should return 1 inserted record")
    assert_equals("Eve", inserted[1].name, "Should set fields from query")
    assert_equals(7, inserted[1].level, "Should set fields from data")

    -- Verify the record was added to collection
    local count_after = #users.data
    assert_equals(2, count_after, "Collection should now have 2 records")

    -- Test with indexed fields
    users:createIndex("email")
    users:upsert({ email = "frank@example.com" }, { name = "Frank", level = 3 })

    local frank = users:get({ email = "frank@example.com" })
    assert_true(frank ~= nil, "Should insert document with indexed field")

    db:close()
end
-- Run all tests
local function run_tests()
    print("=== Running MaraDB Tests ===")

    local tests = {
        test_database_creation,
        test_collection_operations,
        test_basic_crud,
        test_index_creation,
        test_uniqueness_constraint,
        test_string_insert_with_index,
        test_multiple_collection_indexes,
        test_nested_objects,
        test_nested_arrays,
        test_complex_nesting,
        test_nil_values,
        test_nested_nil_values,
        test_indexed_nil_values,
        test_where_with_partial_update,
        test_get_method,
        test_upsert_method
    }

    local passed = 0
    local failed = 0

    for i, test in ipairs(tests) do
        io.write("Running test #" .. i .. "... ")
        local success, err = pcall(test)
        if success then
            print("PASSED")
            passed = passed + 1
        else
            print("FAILED: " .. err)
            failed = failed + 1
        end
    end

    print("\nResults: " .. passed .. " passed, " .. failed .. " failed")
    return failed == 0
end

-- Execute the tests
run_tests()