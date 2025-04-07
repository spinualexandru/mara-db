local maradb = {}

function maradb:decode_json_string(str)
    local pos = 1

    local parse = {}

    function parse:skipWhitespace()
        while pos <= #str and str:sub(pos, pos):match("%s") do
            pos = pos + 1
        end
    end

    function parse:object()
        local obj = {}
        pos = pos + 1 -- Skip '{'

        self:skipWhitespace()
        if str:sub(pos, pos) == "}" then
            pos = pos + 1
            return obj
        end

        while true do
            self:skipWhitespace()

            if str:sub(pos, pos) ~= "\"" then
                error("Expected key string in object at position " .. pos)
            end

            local key = self:string()

            self:skipWhitespace()
            if str:sub(pos, pos) ~= ":" then
                error("Expected ':' after key at position " .. pos)
            end
            pos = pos + 1 -- Skip ':'

            obj[key] = self:value()

            self:skipWhitespace()
            if str:sub(pos, pos) == "}" then
                pos = pos + 1
                break
            end

            if str:sub(pos, pos) ~= "," then
                error("Expected ',' or '}' after object value at position " .. pos)
            end
            pos = pos + 1 -- Skip ','
        end

        return obj
    end

    function parse:array()
        local arr = {}
        pos = pos + 1 -- Skip '['

        self:skipWhitespace()
        if str:sub(pos, pos) == "]" then
            pos = pos + 1
            return arr
        end

        while true do
            table.insert(arr, self:value())

            self:skipWhitespace()
            if str:sub(pos, pos) == "]" then
                pos = pos + 1
                break
            end

            if str:sub(pos, pos) ~= "," then
                error("Expected ',' or ']' after array value at position " .. pos)
            end
            pos = pos + 1 -- Skip ','
        end

        return arr
    end

    function parse:string()
        local start_pos = pos + 1
        local end_pos = str:find("\"", start_pos)
        if not end_pos then
            error("Unclosed string at position " .. start_pos)
        end
        pos = end_pos + 1
        return str:sub(start_pos, end_pos - 1)
    end

    function parse:number()
        local start_pos = pos
        while pos <= #str and str:sub(pos, pos):match("[%d%.%-eE%+]") do
            pos = pos + 1
        end
        return tonumber(str:sub(start_pos, pos - 1))
    end

    function parse:value()
        self:skipWhitespace()

        local char = str:sub(pos, pos)
        if char == "{" then
            return self:object()
        elseif char == "[" then
            return self:array()
        elseif char == "\"" then
            return self:string()
        elseif char:match("[%d%-]") then
            return self:number()
        elseif str:sub(pos, pos + 3) == "true" then
            pos = pos + 4
            return true
        elseif str:sub(pos, pos + 4) == "false" then
            pos = pos + 5
            return false
        elseif str:sub(pos, pos + 3) == "null" then
            pos = pos + 4
            return nil
        else
            error("Invalid JSON value at position " .. pos)
        end
    end

    return parse:value()
end

function maradb:decode_json(filename)
    local file_content = self:read_file(filename)
    if not file_content then
        return nil
    end
    return self:decode_json_string(file_content)
end

function maradb:encode_value(value)
    if type(value) == "table" then
        if #value > 0 then
            return maradb:encode_array(value)
        else
            return maradb:encode_object(value)
        end
    elseif type(value) == "string" then
        return "\"" .. value .. "\""
    elseif type(value) == "number" or type(value) == "boolean" then
        return tostring(value)
    elseif value == nil then
        return "null"
    else
        error("Unsupported data type: " .. type(value))
    end
end

function maradb:encode_object(obj)
    local result = "{"
    for key, value in pairs(obj) do
        result = result .. "\"" .. key .. "\":" .. maradb:encode_value(value) .. ","
    end
    if result:sub(-1) == "," then
        result = result:sub(1, -2)
    end
    result = result .. "}"
    return result
end

function maradb:encode_array(arr)
    local result = "["
    for _, value in ipairs(arr) do
        result = result .. maradb:encode_value(value) .. ","
    end
    if result:sub(-1) == "," then
        result = result:sub(1, -2)
    end
    result = result .. "]"
    return result
end

function maradb:encode_json(data)
    return maradb:encode_value(data)
end

function maradb:read_file(filename)
    local file = io.open(filename, "r")
    if not file then
        return nil
    end
    local content = file:read("*a")
    file:close()
    return content
end

function maradb:write_file(filename, content)
    local file = io.open(filename, "w")
    if not file then
        return false
    end
    file:write(content)
    file:close()
    return true
end

function maradb.open(db_name)
    local db = {}
    db.name = db_name
    db.collections = {}

    -- Load existing data if the file exists
    local file_content = maradb:read_file(db_name .. ".db")
    if file_content then
        local decoded_data = maradb:decode_json_string(file_content)
        if decoded_data then
            -- First load "indexes" collection if it exists
            if decoded_data["indexes"] then
                db.collections["indexes"] = maradb:collection("indexes", db)
                db.collections["indexes"].data = decoded_data["indexes"]
            end

            -- Then load other collections
            for name, data in pairs(decoded_data) do
                if name ~= "indexes" then
                    db.collections[name] = maradb:collection(name, db)
                    db.collections[name].data = data
                end
            end
        end
    end

    function db:collection(collection_name)
        if not self.collections[collection_name] then
            self.collections[collection_name] = maradb:collection(collection_name, self)
        end
        return self.collections[collection_name]
    end

    function db:save()
        local all_data = {}
        for name, collection in pairs(self.collections) do
            all_data[name] = collection.data
        end
        local json_data = maradb:encode_json(all_data)
        return maradb:write_file(self.name .. ".db", json_data)
    end

    function db:close()
        return self:save()
    end

    return db
end

function maradb:load_indexes(db, collection_name)
    -- Skip index loading for the indexes collection itself
    if collection_name == "indexes" then
        return {}
    end

    local index_map = {}
    if db.collections["indexes"] then
        local indexes = db.collections["indexes"]
        for _, index in ipairs(indexes.data) do
            if index.collection == collection_name then
                index_map[index.field] = {
                    exists = true,
                    unique = index.unique == nil or index.unique -- Default to true if not specified
                }
            end
        end
    end
    return index_map
end

function maradb:collection(collection_name, db)
    if not collection_name then
        return nil
    end

    local collection = {}
    collection.name = collection_name
    collection.data = {}
    collection.db = db
    collection.indexes = self:load_indexes(db, collection_name)

    function collection:createIndex(field, options)
        options = options or {}
        local unique = options.unique
        if unique == nil then
            unique = true
        end -- Default to true

        -- Check if index already exists
        if self.indexes[field] then
            return -- Index already exists, skip creation
        end

        local indexes = self.db:collection("indexes")

        -- Check if index already exists in the database
        local existing = indexes:find({ collection = self.name, field = field })
        if #existing == 0 then
            indexes:insert({
                collection = self.name,
                field = field,
                unique = unique
            })
        end

        self.indexes[field] = { exists = true, unique = unique }
    end

    function collection:insert(data)
        -- Handle string data
        if type(data) == "string" then
            local indexed_fields = {}
            for field, _ in pairs(self.indexes) do
                table.insert(indexed_fields, field)
            end

            if #indexed_fields > 0 then
                local new_data = {}
                new_data[indexed_fields[1]] = data
                data = new_data
            else
                data = { value = data }
            end
        end

        -- Check indexed fields
        for field, config in pairs(self.indexes) do
            if not data[field] then
                error("Missing indexed field: " .. field)
            end

            -- Check uniqueness constraints against existing data
            if config.unique then
                for _, item in ipairs(self.data) do
                    if item[field] == data[field] then
                        error("Duplicate value '" .. tostring(data[field]) ..
                                "' for unique indexed field: " .. field)
                    end
                end
            end
        end

        table.insert(self.data, data)
        self.db:save()
        return #self.data
    end

    -- Other collection methods remain the same
    function collection:find(query)
        local results = {}
        for _, item in ipairs(self.data) do
            local match = true
            for key, value in pairs(query) do
                if item[key] ~= value then
                    match = false
                    break
                end
            end
            if match then
                table.insert(results, item)
            end
        end
        return results
    end

    function collection:where(query, partial_data)
        local results = {}
        for _, item in ipairs(self.data) do
            local match = true
            for key, value in pairs(query) do
                if item[key] ~= value then
                    match = false
                    break
                end
            end
            if match then
                table.insert(results, item)

                -- If partial_data is provided, update matching records
                if partial_data then
                    for key, value in pairs(partial_data) do
                        item[key] = value
                    end
                end
            end
        end

        -- Save changes if updates were made
        if partial_data and #results > 0 then
            self.db:save()
        end

        return results
    end

    -- Method to get only the first match
    function collection:get(query)
        for _, item in ipairs(self.data) do
            local match = true
            for key, value in pairs(query) do
                if item[key] ~= value then
                    match = false
                    break
                end
            end
            if match then
                return item
            end
        end
        return nil
    end

    -- Method to update matching records or insert if none found
    function collection:upsert(query, data)
        local results = self:where(query)

        if #results > 0 then
            -- Update existing records
            for _, item in ipairs(results) do
                for key, value in pairs(data) do
                    item[key] = value
                end
            end
            self.db:save()
            return results
        else
            -- Insert new record
            local new_data = {}

            -- Combine query and data
            for key, value in pairs(query) do
                new_data[key] = value
            end

            for key, value in pairs(data) do
                new_data[key] = value
            end

            local id = self:insert(new_data)
            return { self.data[id] }
        end
    end

    function collection:remove(query)
        for i = #self.data, 1, -1 do
            local item = self.data[i]
            local match = true
            for key, value in pairs(query) do
                if item[key] ~= value then
                    match = false
                    break
                end
            end
            if match then
                table.remove(self.data, i)
            end
        end
        self.db:save()
    end

    function collection:update(query, new_data)
        for _, item in ipairs(self.data) do
            local match = true
            for key, value in pairs(query) do
                if item[key] ~= value then
                    match = false
                    break
                end
            end
            if match then
                for key, value in pairs(new_data) do
                    item[key] = value
                end
            end
        end
        self.db:save()
    end

    function collection:purge()
        self.data = {}
        self.db:save()
    end

    function collection:save(filename)
        local json_data = maradb:encode_json(self.data)
        return maradb:write_file(filename, json_data)
    end

    return collection
end
return maradb