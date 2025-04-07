# MaraDB

MaraDB is a lightweight document database for Lua with JSON persistence. It provides an easy-to-use interface for
storing, retrieving, and manipulating structured data.

## Contents

- [Features](#features)
- [Installation](#installation)
- [Basic Usage](#basic-usage)
    - [Opening a Database](#opening-a-database)
    - [Working with Collections](#working-with-collections)
- [CRUD Operations](#crud-operations)
    - [Insert](#insert)
    - [Query](#query)
    - [Update](#update)
    - [Upsert](#upsert-update-or-insert)
    - [Delete](#delete)
- [Indexing](#indexing)
- [Working with Nested Data](#working-with-nested-data)
- [Complete Example](#complete-example)
- [Love2D Examples](examples/love2d)
    - [Basic Usage in Love2D](examples/love2d/basic.lua)
    - [Game Inventory System](examples/love2d/game_inventory.lua)
    - [Player Save System](examples/love2d/save_system.lua)
    - [High Score System](examples/love2d/high_score.lua)
    - [Dialog and Quest System](examples/love2d/quest_dialog.lua)
- [License](#license)

## Features

- Document-oriented storage
- Collection-based organization
- JSON persistence
- Indexing and unique constraints
- Support for nested objects and arrays
- Full CRUD operations (Create, Read, Update, Delete)

## Installation

Simply include the `maradb.lua` file in your project:

```lua
local maradb = require('maradb')
```

## Basic Usage

### Opening a Database

```lua
-- Open a database (creates it if it doesn't exist)
local db = maradb.open("my_database")

-- Save changes and close when done
db:close()
```

### Working with Collections

```lua
-- Get or create a collection
local users = db:collection("users")

-- Insert a document
users:insert({ name = "Alice", email = "alice@example.com", age = 30 })

-- Close the database when done
db:close()
```

## CRUD Operations

### Insert

```lua
-- Insert a document
users:insert({ name = "Bob", email = "bob@example.com", age = 25 })

-- Insert a simple string value
tags:insert("important")
```

### Query

```lua
-- Find all matching documents
local active_users = users:where({ status = "active" })

-- Get a single matching document
local user = users:get({ email = "alice@example.com" })

-- Find with multiple conditions
local admins = users:where({ status = "active", role = "admin" })
```

### Update

```lua
-- Update all matching documents
users:update({ status = "active" }, { last_seen = "today" })

-- Find and update in one operation (returns updated documents)
local updated_users = users:where({ status = "active" }, { level = 2 })
```

### Upsert (Update or Insert)

```lua
-- Update if exists, insert if not
users:upsert({ email = "charlie@example.com" }, { name = "Charlie", level = 1 })
```

### Delete

```lua
-- Remove all matching documents
users:remove({ status = "inactive" })

-- Clear all documents in collection
users:purge()
```

## Indexing

```lua
-- Create an index (default: unique = true)
users:createIndex("email")

-- Create a non-unique index
users:createIndex("status", { unique = false })
```

## Working with Nested Data

```lua
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

-- Query nested document
local post = posts:get({ "author.name" = "Alice" })
```

## Complete Example

```lua
local maradb = require('maradb')

-- Open database
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
print("Admin users:", #admin_users)

-- Update data
users:update({ name = "Bob" }, { status = "active" })

-- Upsert data
users:upsert({ email = "charlie@example.com" }, {
    name = "Charlie",
    role = "contributor",
    status = "new"
})

-- Save changes
db:close()
```

## License

MIT License with Attribution Clause. See the [LICENSE](LICENSE) file for details.

When used in commercial products, attribution is required: "This product/service uses software created by
Alexandru-Mihai Spinu."