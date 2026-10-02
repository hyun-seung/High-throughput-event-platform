-- KEYS[1] = client fixed-window hash; KEYS[2] = client/type/month quota counter
-- ARGV[1] = window maximum; ARGV[2] = monthly maximum
local now = redis.call('TIME')
local window = math.floor(tonumber(now[1]) / 10)
local tpsUsage = redis.call('HINCRBY', KEYS[1], tostring(window), 1)
redis.call('HDEL', KEYS[1], tostring(window - 2))
redis.call('EXPIRE', KEYS[1], 30)
local monthlyUsage = redis.call('INCR', KEYS[2])
if monthlyUsage == 1 then redis.call('EXPIRE', KEYS[2], 5702400) end
if tpsUsage > tonumber(ARGV[1]) then return {1, tpsUsage, monthlyUsage} end
if monthlyUsage > tonumber(ARGV[2]) then return {2, tpsUsage, monthlyUsage} end
return {0, tpsUsage, monthlyUsage}
