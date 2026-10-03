-- KEYS[1] = event:usage:{client:<id>}:policy
-- KEYS[2] = event:usage:{client:<id>}:10s
-- KEYS[3] = event:usage:{client:<id>}:month:<yyyy-MM>:<eventType>
-- ARGV[1] = eventType (GENERAL, NOTI, ADV, ALERT)
-- Policy is configured independently of the customer contract table.

local tpsLimit = tonumber(redis.call('HGET', KEYS[1], 'tpsLimit'))
local monthlyLimit = tonumber(redis.call('HGET', KEYS[1], 'quota' .. ARGV[1]))
if redis.call('EXISTS', KEYS[1]) == 0 then return {3, -1, -1} end
if not tpsLimit or tpsLimit <= 0 or not monthlyLimit or monthlyLimit <= 0 then
    return {4, -1, -1}
end

local now = redis.call('TIME')
local window = math.floor(tonumber(now[1]) / 10)
local currentWindow = tostring(window)
local tpsUsage
if redis.call('HGET', KEYS[2], 'window') == currentWindow then
    tpsUsage = redis.call('HINCRBY', KEYS[2], 'count', 1)
else
    redis.call('HSET', KEYS[2], 'window', currentWindow, 'count', 1)
    tpsUsage = 1
end
redis.call('EXPIRE', KEYS[2], 30)

local monthlyUsage = redis.call('INCR', KEYS[3])
if monthlyUsage == 1 then redis.call('EXPIRE', KEYS[3], 5702400) end

if tpsUsage > tpsLimit * 10 then return {1, tpsUsage, monthlyUsage} end
if monthlyUsage > monthlyLimit then return {2, tpsUsage, monthlyUsage} end
return {0, tpsUsage, monthlyUsage}
