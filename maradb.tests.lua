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

-- Cleanup function
local function cleanup()
    os.remove("test_db.db")
    os.remove("my_database.db")
    os.remove("blog.db")
end

-- 1. Test opening a database
local function test_opening_database()
    cleanup()

    -- Open a database (creates it if it doesn't exist)
    local db = maradb.open("my_database")
    assert_true(db ~= nil, "Database should be created")

    -- Save changes and close when done
    local result = db:close()
    assert_true(result, "Database should close successfully")

    -- Verify database file was created
    local file = io.open("my_database.db", "r")
    assert_true(file ~= nil, "Database file should exist")
    file:close()
end

-- 2. Test working with collections
local function test_collections()
    cleanup()

    local db = maradb.open("test_db")

    -- Get or create a collection
    local users = db:collection("users")
    assert_true(users ~= nil, "Collection should be created")

    -- Insert a document
    users:insert({ name = "Alice", email = "alice@example.com", age = 30 })
    assert_equals(1, #users.data, "Collection should have one document")

    -- Verify document properties
    assert_equals("Alice", users.data[1].name, "Document should have correct name")
    assert_equals("alice@example.com", users.data[1].email, "Document should have correct email")
    assert_equals(30, users.data[1].age, "Document should have correct age")

    db:close()
end

-- 3. Test insert operations
local function test_insert()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")
    local tags = db:collection("tags")

    -- Insert a document
    users:insert({ name = "Bob", email = "bob@example.com", age = 25 })
    assert_equals(1, #users.data, "Collection should have one document")
    assert_equals("Bob", users.data[1].name, "Document should have correct data")

    -- Insert a simple string value
    tags:insert("important")
    assert_equals(1, #tags.data, "Tags collection should have one item")
    assert_true(tags.data[1].value == "important" or tags.data[1].name == "important",
            "Tag value should be stored correctly")

    db:close()
end

-- 4. Test query operations
local function test_queries()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    -- Insert test data
    users:insert({ name = "Alice", email = "alice@example.com", status = "active", role = "admin" })
    users:insert({ name = "Bob", email = "bob@example.com", status = "active", role = "editor" })
    users:insert({ name = "Charlie", email = "charlie@example.com", status = "inactive", role = "user" })

    -- Find all matching documents
    local active_users = users:where({ status = "active" })
    assert_equals(2, #active_users, "Should find 2 active users")

    -- Get a single matching document
    local user = users:get({ email = "alice@example.com" })
    assert_true(user ~= nil, "Should find Alice")
    assert_equals("Alice", user.name, "Should return correct user")

    -- Find with multiple conditions
    local admins = users:where({ status = "active", role = "admin" })
    assert_equals(1, #admins, "Should find 1 admin")
    assert_equals("Alice", admins[1].name, "Admin should be Alice")

    db:close()
end

-- 5. Test update operations
local function test_updates()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    -- Insert test data
    users:insert({ name = "Alice", status = "active", level = 1 })
    users:insert({ name = "Bob", status = "active", level = 1 })
    users:insert({ name = "Charlie", status = "inactive", level = 1 })

    -- Update all matching documents
    users:update({ status = "active" }, { last_seen = "today" })

    -- Verify updates
    local alice = users:get({ name = "Alice" })
    assert_equals("today", alice.last_seen, "Alice should have last_seen field updated")

    local charlie = users:get({ name = "Charlie" })
    assert_true(charlie.last_seen == nil, "Charlie should not have last_seen field")

    -- Find and update in one operation
    local updated_users = users:where({ status = "active" }, { level = 2 })
    assert_equals(2, #updated_users, "Should update and return 2 users")
    assert_equals(2, updated_users[1].level, "First user should have updated level")

    -- Verify all updates persisted
    local bob = users:get({ name = "Bob" })
    assert_equals(2, bob.level, "Bob's level should be updated")
    assert_equals("today", bob.last_seen, "Bob's last_seen should be updated")

    db:close()
end

-- 6. Test upsert operations
local function test_upsert()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    -- Insert initial data
    users:insert({ name = "Alice", email = "alice@example.com", level = 5 })

    -- Update existing record
    local updated = users:upsert({ email = "alice@example.com" }, { name = "Alice Updated", level = 6 })
    assert_equals(1, #updated, "Should return 1 updated record")
    assert_equals("Alice Updated", updated[1].name, "Name should be updated")
    assert_equals(6, updated[1].level, "Level should be updated")

    -- Insert new record
    local inserted = users:upsert({ email = "charlie@example.com" }, { name = "Charlie", level = 1 })
    assert_equals(1, #inserted, "Should return 1 inserted record")
    assert_equals("Charlie", inserted[1].name, "Should insert with correct name")

    -- Verify collection state
    assert_equals(2, #users.data, "Collection should have 2 records")
    local alice = users:get({ email = "alice@example.com" })
    assert_equals("Alice Updated", alice.name, "Alice's update should persist")

    db:close()
end

-- 7. Test delete operations
local function test_delete()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    -- Insert test data
    users:insert({ name = "Alice", status = "active" })
    users:insert({ name = "Bob", status = "active" })
    users:insert({ name = "Charlie", status = "inactive" })

    -- Remove all matching documents
    users:remove({ status = "inactive" })
    assert_equals(2, #users.data, "Should have 2 documents after remove")

    local charlie = users:get({ name = "Charlie" })
    assert_true(charlie == nil, "Charlie should be removed")

    -- Clear all documents in collection
    users:purge()
    assert_equals(0, #users.data, "Collection should be empty after purge")

    db:close()
end

-- 8. Test indexing
local function test_indexing()
    cleanup()

    local db = maradb.open("test_db")
    local users = db:collection("users")

    -- Create an index (default: unique = true)
    users:createIndex("email")

    -- Insert with indexed field
    users:insert({ name = "Alice", email = "alice@example.com" })

    -- Test uniqueness constraint
    local success, err = pcall(function()
        users:insert({ name = "Alice Clone", email = "alice@example.com" })
    end)
    assert_true(not success, "Should reject duplicate email")

    -- Create a non-unique index
    local tags = db:collection("tags")
    tags:createIndex("category", { unique = false })

    -- Insert documents with same non-unique indexed field
    tags:insert({ name = "Tag1", category = "important" })
    tags:insert({ name = "Tag2", category = "important" })
    assert_equals(2, #tags.data, "Should allow duplicate non-unique indexed field")

    db:close()
end

-- 9. Test nested data
local function test_nested_data()
    cleanup()

    local db = maradb.open("test_db")
    local posts = db:collection("posts")

    -- Insert document with nested structure
    posts:insert({
        title = "Hello World",
        author = {
            name = "Alice",
            email = "alice@example.com"
        },
        tags = { "lua", "database", "tutorial" },
        comments = {
            { user = "Bob", text = "Great post!" },
            { user = "Charlie", text = "Thanks for sharing!" }
        }
    })

    -- Verify nested data was stored correctly
    assert_equals(1, #posts.data, "Should have one post")
    local post = posts.data[1]

    assert_equals("Hello World", post.title, "Title should be stored correctly")
    assert_equals("Alice", post.author.name, "Nested author name should be stored")
    assert_equals("alice@example.com", post.author.email, "Nested author email should be stored")

    assert_equals(3, #post.tags, "Should have 3 tags")
    assert_equals("lua", post.tags[1], "First tag should be stored correctly")

    assert_equals(2, #post.comments, "Should have 2 comments")
    assert_equals("Bob", post.comments[1].user, "First comment user should be stored")
    assert_equals("Thanks for sharing!", post.comments[2].text, "Second comment text should be stored")

    db:close()
end

-- 10. Test complete example
local function test_complete_example()
    cleanup()

    local db = maradb.open("blog")

    -- Create collections
    local users = db:collection("users")
    local posts = db:collection("posts")

    -- Create indexes
    users:createIndex("email")
    posts:createIndex("slug")

    -- Insert users
    users:insert({ name = "Alice", email = "alice@example.com", role = "admin" })
    users:insert({ name = "Bob", email = "bob@example.com", role = "editor" })

    -- Insert posts
    posts:insert({
        title = "Getting Started with MaraDB",
        slug = "getting-started",
        content = "This is a tutorial on using MaraDB...",
        author = "alice@example.com",
        tags = { "tutorial", "database" }
    })

    -- Query data
    local admin_users = users:where({ role = "admin" })
    assert_equals(1, #admin_users, "Should find 1 admin user")

    -- Update data
    users:update({ name = "Bob" }, { status = "active" })
    local bob = users:get({ name = "Bob" })
    assert_equals("active", bob.status, "Bob's status should be updated")

    -- Upsert data
    users:upsert({ email = "charlie@example.com" }, {
        name = "Charlie",
        role = "contributor",
        status = "new"
    })

    local charlie = users:get({ email = "charlie@example.com" })
    assert_true(charlie ~= nil, "Charlie should be inserted")
    assert_equals("contributor", charlie.role, "Charlie should have correct role")

    db:close()

    -- Verify persistence
    local db2 = maradb.open("blog")
    local users2 = db2:collection("users")
    assert_equals(3, #users2.data, "Database should persist 3 users")
    db2:close()
end

-- Run all tests
local function run_tests()
    print("=== Running MaraDB README Examples Tests ===")

    local tests = {
        test_opening_database,
        test_collections,
        test_insert,
        test_queries,
        test_updates,
        test_upsert,
        test_delete,
        test_indexing,
        test_nested_data,
        test_complete_example
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

-- Execute all tests
run_tests()