local maradb = require('maradb')

local db = nil
local dialogs = nil
local quests = nil
local questState = nil
local currentDialog = nil
local currentQuest = nil

function love.load()
    db = maradb.open("game_content")

    -- Initialize collections
    dialogs = db:collection("dialogs")
    quests = db:collection("quests")
    questState = db:collection("quest_state")

    -- Create some example dialog if collection is empty
    if #dialogs.data == 0 then
        dialogs:insert({
            npc = "Blacksmith",
            id = "greeting",
            text = "Welcome, adventurer! Need any weapons or armor?",
            options = {
                { text = "Show me your wares", next = "shop" },
                { text = "I need a special sword", next = "quest_intro", requires_quest = "none" }
            }
        })

        dialogs:insert({
            npc = "Blacksmith",
            id = "shop",
            text = "Here's what I have available today.",
            options = {
                { text = "I'll take a look", action = "open_shop" },
                { text = "Maybe later", next = "greeting" }
            }
        })

        dialogs:insert({
            npc = "Blacksmith",
            id = "quest_intro",
            text = "I could forge you a special sword, but I need rare materials. Bring me 3 iron ingots and a magic crystal.",
            options = {
                { text = "I'll find them for you", next = "quest_accept", action = "start_quest", quest = "special_sword" },
                { text = "That's too difficult", next = "greeting" }
            }
        })
    end

    -- Create example quests if collection is empty
    if #quests.data == 0 then
        quests:insert({
            id = "special_sword",
            title = "The Special Sword",
            description = "Collect materials for the blacksmith to forge a special sword",
            objectives = {
                { id = "iron_ingots", name = "Iron Ingots", required = 3, current = 0 },
                { id = "magic_crystal", name = "Magic Crystal", required = 1, current = 0 }
            },
            rewards = {
                { type = "item", id = "special_sword", quantity = 1 },
                { type = "gold", amount = 100 }
            }
        })
    end

    -- Load quest state for active quests
    local activeQuests = questState:where({ active = true })
    if #activeQuests > 0 then
        currentQuest = activeQuests[1]
    end
end

function showDialog(dialogId)
    currentDialog = dialogs:get({ id = dialogId })
    if not currentDialog then
        print("Dialog not found: " .. dialogId)
        return false
    end
    return true
end

function selectDialogOption(optionIndex)
    if not currentDialog or not currentDialog.options or not currentDialog.options[optionIndex] then
        return false
    end

    local option = currentDialog.options[optionIndex]

    -- Handle quest requirements
    if option.requires_quest then
        if option.requires_quest ~= "none" then
            local quest = questState:get({ id = option.requires_quest, active = true })
            if not quest then
                return false
            end
        end
    end

    -- Handle actions
    if option.action then
        if option.action == "start_quest" and option.quest then
            startQuest(option.quest)
        elseif option.action == "complete_quest" and option.quest then
            completeQuest(option.quest)
        elseif option.action == "open_shop" then
            -- Shop logic would go here
            print("Opening shop...")
        end
    end

    -- Move to next dialog
    if option.next then
        return showDialog(option.next)
    end

    return true
end

function startQuest(questId)
    local questData = quests:get({ id = questId })
    if not questData then
        return false
    end

    -- Check if quest is already active
    local existing = questState:get({ id = questId })
    if existing and existing.active then
        return false
    end

    -- Create a new quest state entry
    local newQuestState = {
        id = questId,
        title = questData.title,
        active = true,
        completed = false,
        objectives = questData.objectives,
        rewards = questData.rewards,
        started_at = os.time()
    }

    if existing then
        questState:update({ id = questId }, newQuestState)
    else
        questState:insert(newQuestState)
    end

    currentQuest = newQuestState
    print("Started quest: " .. questData.title)
    return true
end

function completeQuest(questId)
    local quest = questState:get({ id = questId, active = true })
    if not quest then
        return false
    end

    -- Check if all objectives are complete
    for _, objective in ipairs(quest.objectives) do
        if objective.current < objective.required then
            return false
        end
    end

    -- Update quest state
    questState:update({ id = questId }, {
        active = false,
        completed = true,
        completed_at = os.time()
    })

    -- Give rewards
    for _, reward in ipairs(quest.rewards) do
        if reward.type == "item" then
            -- Add item to inventory logic would go here
            print("Received reward: " .. reward.quantity .. "x " .. reward.id)
        elseif reward.type == "gold" then
            -- Add gold logic would go here
            print("Received " .. reward.amount .. " gold")
        end
    end

    currentQuest = nil
    return true
end

function updateQuestObjective(questId, objectiveId, amount)
    local quest = questState:get({ id = questId, active = true })
    if not quest then
        return false
    end

    for i, objective in ipairs(quest.objectives) do
        if objective.id == objectiveId then
            local newAmount = objective.current + amount
            if newAmount > objective.required then
                newAmount = objective.required
            end

            quest.objectives[i].current = newAmount

            -- Update in database
            questState:update({ id = questId }, { objectives = quest.objectives })

            -- Check if quest can be completed
            local allComplete = true
            for _, obj in ipairs(quest.objectives) do
                if obj.current < obj.required then
                    allComplete = false
                    break
                end
            end

            if allComplete then
                print("All objectives complete! Quest can be turned in.")
            end

            return true
        end
    end

    return false
end

function love.draw()
    -- Display current dialog
    if currentDialog then
        love.graphics.setColor(0, 0, 0, 0.8)
        love.graphics.rectangle("fill", 50, 350, 700, 200)
        love.graphics.setColor(1, 1, 1)

        love.graphics.print(currentDialog.npc .. ":", 70, 370)
        love.graphics.print(currentDialog.text, 70, 400)

        if currentDialog.options then
            for i, option in ipairs(currentDialog.options) do
                love.graphics.print(i .. ". " .. option.text, 70, 430 + (i - 1) * 20)
            end
        end
    end

    -- Display current quest
    if currentQuest then
        love.graphics.setColor(0, 0, 0, 0.8)
        love.graphics.rectangle("fill", 50, 50, 300, 200)
        love.graphics.setColor(1, 1, 1)

        love.graphics.print("Active Quest: " .. currentQuest.title, 70, 70)

        for i, objective in ipairs(currentQuest.objectives) do
            love.graphics.print(objective.name .. ": " .. objective.current .. "/" .. objective.required,
                    70, 100 + (i - 1) * 20)
        end
    end
end

function love.keypressed(key)
    if key == "1" or key == "2" or key == "3" then
        selectDialogOption(tonumber(key))
    elseif key == "d" then
        showDialog("greeting")
    elseif key == "q" then
        if currentQuest and currentQuest.id == "special_sword" then
            updateQuestObjective("special_sword", "iron_ingots", 1)
            print("Added progress to iron ingots objective")
        end
    elseif key == "c" then
        if currentQuest then
            completeQuest(currentQuest.id)
        end
    end
end

function love.quit()
    db:close()
end