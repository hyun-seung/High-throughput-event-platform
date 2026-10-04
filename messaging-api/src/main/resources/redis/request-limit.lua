-- KEYS[1] : request-control:{userId}:policy
-- KEYS[2] : request-control:{userId}:bucket
-- KEYS[3] : request-control:{userId}:quota:{yyyyMM}

local STATUS_ALLOWED = 0
local STATUS_BLOCKED = 1
local STATUS_TPS_LIMIT_EXCEEDED = 2
local STATUS_MONTHLY_QUOTA_EXCEEDED = 3
local STATUS_POLICY_NOT_FOUND = 4
local STATUS_POLICY_INVALID = 5

local policyKey = KEYS[1]
local bucketKey = KEYS[2]
local quotaKey = KEYS[3]

-- 정책 존재 여부 확인
if redis.call('EXISTS', policyKey) == 0 then
    return {STATUS_POLICY_NOT_FOUND, -1, -1, -1}
end

-- 정책 일괄 조회
local policy = redis.call(
    'HMGET',
    policyKey,
    'blocked',
    'tpsEnabled',
    'requestsPerSecond',
    'burstCapacity',
    'quotaEnabled',
    'monthlyLimit'
)

local blocked = policy[1]
local tpsEnabled = policy[2]
local requestsPerSecond = tonumber(policy[3])
local burstCapacity = tonumber(policy[4])
local quotaEnabled = policy[5]
local monthlyLimit = tonumber(policy[6])

-- 차단 정책 확인
if blocked == 'true' then
    return {STATUS_BLOCKED, -1, -1, monthlyLimit or -1}
end

-- TPS 정책 유효성 확인
if tpsEnabled == 'true' and (requestsPerSecond == nil or requestsPerSecond <= 0 or burstCapacity == nil or burstCapacity <= 0) then
    return {STATUS_POLICY_INVALID, -1, -1, monthlyLimit or -1}
end

-- Quota 정책 유효성 확인
if quotaEnabled == 'true' and (monthlyLimit == nil or monthlyLimit <= 0) then
    return {STATUS_POLICY_INVALID, -1, -1, -1}
end

-- 현재 월 Quota 사용량
local monthlyUsage = tonumber(redis.call('GET', quotaKey)) or 0

-- Quota 초과 여부 확인
if quotaEnabled == 'true' and monthlyUsage >= monthlyLimit then
    return {STATUS_MONTHLY_QUOTA_EXCEEDED, -1, monthlyUsage, monthlyLimit}
end

local remainingTokens = -1

if tpsEnabled == 'true' then
    -- 모든 인스턴스가 Redis 서버 시간을 기준으로 Token refill 계산
    local redisTime = redis.call('TIME')
    local currentTimeMillis = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)

    local bucket = redis.call('HMGET', bucketKey, 'tokens', 'lastRefillTime')
    local tokens = tonumber(bucket[1])
    local lastRefillTime = tonumber(bucket[2])

    -- 최초 요청이면 Bucket을 가득 채운 상태로 시작
    if tokens == nil or lastRefillTime == nil then
        tokens = burstCapacity
        lastRefillTime = currentTimeMillis
    else
        local elapsedMillis = math.max(0, currentTimeMillis - lastRefillTime)
        local refillTokens = elapsedMillis * requestsPerSecond / 1000

        tokens = math.min(burstCapacity, tokens + refillTokens)
        lastRefillTime = currentTimeMillis
    end

    -- 사용 가능한 Token이 없으면 TPS 제한
    if tokens < 1 then
        redis.call('HSET', bucketKey, 'tokens', tokens, 'lastRefillTime', lastRefillTime)
        return {STATUS_TPS_LIMIT_EXCEEDED, 0, monthlyUsage, monthlyLimit or -1}
    end

    -- 정상 요청이므로 Token 1개 차감
    tokens = tokens - 1
    remainingTokens = math.floor(tokens)

    redis.call('HSET', bucketKey, 'tokens', tokens, 'lastRefillTime', lastRefillTime)
end

-- 정상적으로 허용된 요청만 Quota 사용량 증가
if quotaEnabled == 'true' then
    monthlyUsage = redis.call('INCR', quotaKey)
end

return {STATUS_ALLOWED, remainingTokens, monthlyUsage, monthlyLimit or -1}