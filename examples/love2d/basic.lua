local maradb = require('maradb')

-- Game state variables
local gameState = {}
local db = nil
local players = nil
local settings = nil

function love.load()
    -- Initialize the database
    db = maradb.open("game_data")

    -- Get or create collections
    players = db:collection("players")
    settings = db:collection("settings")

    -- Create indexes for faster lookups
    players:createIndex("username", { unique = true })

    -- Load default settings if none exist
    if #settings.data == 0 then
        settings:insert({
            music_volume = 0.8,
            sfx_volume = 1.0,
            fullscreen = false,
            resolution = "1280x720"
        })
    end

    -- Load game state
    gameState.settings = settings.data[1]

    -- Apply settings
    love.audio.setVolume(gameState.settings.music_volume)
    love.window.setFullscreen(gameState.settings.fullscreen)
end

function love.update(dt)
    -- Game logic here
end

function love.draw()
    -- Render game here
    love.graphics.print("MaraDB Example with Love2D", 10, 10)
    love.graphics.print("Music Volume: " .. gameState.settings.music_volume, 10, 30)
end

function love.quit()
    -- Save game data before closing
    db:close()
end