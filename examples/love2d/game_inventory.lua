local maradb = require('maradb')

local db = nil
local items = nil
local playerInventory = nil
local player = {
    name = "Hero",
    gold = 100,
    inventory = {}
}

function love.load()
    db = maradb.open("game_inventory")

    -- Create item database with predefined items
    items = db:collection("items")
    items:createIndex("id", { unique = true })

    -- Initialize items if empty
    if #items.data == 0 then
        items:insert({ id = "potion", name = "Health Potion", type = "consumable", value = 10, effect = "heal", power = 20 })
        items:insert({ id = "sword", name = "Iron Sword", type = "weapon", value = 50, damage = 15 })
        items:insert({ id = "shield", name = "Wooden Shield", type = "armor", value = 30, defense = 10 })
        items:insert({ id = "key", name = "Rusty Key", type = "key", value = 5 })
    end

    -- Player inventory collection
    playerInventory = db:collection("inventory")

    -- Load player inventory
    local savedInventory = playerInventory:where({ player = player.name })
    for _, item in ipairs(savedInventory) do
        table.insert(player.inventory, {
            id = item.item_id,
            quantity = item.quantity
        })
    end
end

function addItemToInventory(itemId, quantity)
    quantity = quantity or 1

    -- Check if item exists in the database
    local itemData = items:get({ id = itemId })
    if not itemData then
        print("Item does not exist: " .. itemId)
        return false
    end

    -- Update existing item in inventory if found
    for i, item in ipairs(player.inventory) do
        if item.id == itemId then
            player.inventory[i].quantity = player.inventory[i].quantity + quantity

            -- Update database
            playerInventory:update({ player = player.name, item_id = itemId }, {
                quantity = player.inventory[i].quantity
            })
            return true
        end
    end

    -- Add new item to inventory
    table.insert(player.inventory, { id = itemId, quantity = quantity })

    -- Save to database
    playerInventory:insert({
        player = player.name,
        item_id = itemId,
        quantity = quantity
    })

    return true
end

function removeItemFromInventory(itemId, quantity)
    quantity = quantity or 1

    for i, item in ipairs(player.inventory) do
        if item.id == itemId then
            if item.quantity <= quantity then
                -- Remove the item completely
                table.remove(player.inventory, i)
                playerInventory:remove({ player = player.name, item_id = itemId })
            else
                -- Reduce quantity
                item.quantity = item.quantity - quantity
                playerInventory:update({ player = player.name, item_id = itemId }, {
                    quantity = item.quantity
                })
            end
            return true
        end
    end

    return false
end

function getItemDetails(itemId)
    return items:get({ id = itemId })
end

function love.keypressed(key)
    if key == "p" then
        addItemToInventory("potion", 1)
        print("Added potion to inventory")
    elseif key == "s" then
        addItemToInventory("sword", 1)
        print("Added sword to inventory")
    elseif key == "u" then
        if removeItemFromInventory("potion", 1) then
            print("Used a potion")
        else
            print("No potions in inventory")
        end
    end
end

function love.quit()
    db:close()
end