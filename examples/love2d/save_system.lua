local maradb = require('maradb')

-- Game state
local currentPlayer = nil
local db = nil
local players = nil

function love.load()
    db = maradb.open("player_saves")
    players = db:collection("players")
    players:createIndex("name")
end

function createNewPlayer(name)
    -- Check if player already exists
    local existing = players:get({ name = name })

    if existing then
        return existing
    end

    -- Create new player data
    local newPlayer = {
        name = name,
        level = 1,
        health = 100,
        experience = 0,
        inventory = {},
        position = { x = 100, y = 100 },
        created_at = os.time()
    }

    players:insert(newPlayer)
    return newPlayer
end

function savePlayerProgress()
    if currentPlayer then
        -- Update player data in database
        players:update({ name = currentPlayer.name }, {
            level = currentPlayer.level,
            health = currentPlayer.health,
            experience = currentPlayer.experience,
            inventory = currentPlayer.inventory,
            position = currentPlayer.position,
            last_save = os.time()
        })
    end
end

function loadPlayerByName(name)
    local player = players:get({ name = name })
    if player then
        currentPlayer = player
        return true
    end
    return false
end

-- Example usage in Love2D functions
function love.keypressed(key)
    if key == "s" then
        savePlayerProgress()
        print("Game saved!")
    elseif key == "n" then
        currentPlayer = createNewPlayer("Player" .. math.random(1000))
        print("Created new player: " .. currentPlayer.name)
    elseif key == "l" then
        if currentPlayer then
            loadPlayerByName(currentPlayer.name)
            print("Loaded player: " .. currentPlayer.name)
        end
    end
end

function love.quit()
    savePlayerProgress()
    db:close()
end