local maradb = require('maradb')

local db = nil
local highScores = nil
local currentScore = 0
local playerName = "Player"

function love.load()
    db = maradb.open("game_scores")
    highScores = db:collection("scores")

    -- Create index for faster sorting/searching
    highScores:createIndex("score", { unique = false })

    resetGame()
end

function resetGame()
    currentScore = 0
end

function incrementScore(points)
    currentScore = currentScore + points
end

function saveHighScore(name, score)
    highScores:insert({
        player = name,
        score = score,
        date = os.date("%Y-%m-%d %H:%M:%S")
    })
end

function getHighScores(limit)
    limit = limit or 10

    -- Get all scores
    local allScores = highScores.data

    -- Sort by score (descending)
    table.sort(allScores, function(a, b)
        return a.score > b.score
    end)

    -- Return top scores
    local results = {}
    for i = 1, math.min(limit, #allScores) do
        table.insert(results, allScores[i])
    end

    return results
end

function love.update(dt)
    -- Simulate scoring points during gameplay
    if love.keyboard.isDown("space") then
        incrementScore(1)
    end
end

function love.draw()
    -- Display current score
    love.graphics.print("Current Score: " .. currentScore, 10, 10)

    -- Display high scores
    love.graphics.print("High Scores:", 10, 50)

    local topScores = getHighScores(5)
    for i, score in ipairs(topScores) do
        love.graphics.print(i .. ". " .. score.player .. " - " .. score.score, 10, 50 + i * 20)
    end

    -- Instructions
    love.graphics.print("Space: Add points | R: Reset | S: Save score", 10, 200)
end

function love.keypressed(key)
    if key == "r" then
        resetGame()
    elseif key == "s" then
        saveHighScore(playerName, currentScore)
        print("Score saved: " .. currentScore)
    end
end

function love.quit()
    db:close()
end