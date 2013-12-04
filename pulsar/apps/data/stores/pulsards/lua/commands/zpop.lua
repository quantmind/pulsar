-- REDIS ZPOP command. you can pop one or more elements form the sorted set
-- either by range or by score
local key = KEYS[1]
local start = ARGV[2]
local stop = ARGV[3]
local desc = ARGV[4] + 0
local args = {start, stop}
local range

if ARGV[5] + 0 == 1 then
    args[3] = 'withscores'
end

-- POP by RANK
if ARGV[1] == 'rank' then
    if desc == 1 then
        range = redis.call('ZREVRANGE', key, unpack(args))
    else
        range = redis.call('ZRANGE', key, unpack(args))
    end
    redis.call('ZREMRANGEBYRANK', key, start, stop)
-- POP by SCORE
else
    if desc == 1 then
        range = redis.call('ZREVRANGEBYSCORE', key, unpack(args))
    else
        range = redis.call('ZRANGEBYSCORE', key, unpack(args))
    end
    redis.call('ZREMRANGEBYSCORE', key, start, stop)
end

return range