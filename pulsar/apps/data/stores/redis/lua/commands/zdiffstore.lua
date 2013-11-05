-- Implements the ZDIFFSTORE command
local dest = KEYS[1]
local withscores = ARGV[1]
local key = KEYS[2]
if dest ~= key then --  Small optimization which takes care of the -= operation
    redis.call('del', dest)
    local data = redis.call('zrange',key,0,-1,'WITHSCORES')
    local i = 0
    while i < # data do
        redis.call('zadd',dest,data[i+2],data[i+1])
        i = i + 2
    end
end
local i = 2
if withscores == 'withscores' then    -- REMOVE ONLY IF SUBTRACTING SCORES IS EQUAL TO 0
    while i < # KEYS do
        local data = redis.call('zrange',KEYS[i+1],0,-1,'WITHSCORES')
        i = i + 1
        local j = 0
        while j < # data do
            local value, score = data[j+1], data[j+2]
            j = j + 2
            if redis.call('zscore', dest, value) then
                redis.call('zincrby', dest, -score, value)
            end
        end
    end
    redis.call('zremrangebyscore', dest, 0, 0)
else    -- REMOVE REGARDLESS OF SCORE
    while i < # KEYS do
        local data = redis.call('zrange',KEYS[i+1],0,-1)
        i = i + 1
        for _,value in pairs(data) do
            redis.call('zrem',dest,value)
        end
    end
end
return redis.call('zcard', dest)

