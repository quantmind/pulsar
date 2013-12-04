-- Collection of utilities used across scripts.
-- Included in all scripts
-- SCRIPT_START_TIME = os.clock()
local type_table = {}
type_table['set'] = 'scard'
type_table['zset'] = 'zcard'
type_table['list'] = 'llen'
type_table['hash'] = 'hlen'
type_table['ts'] = 'tslen'
type_table['string'] = 'strlen'

local function redis_result(result)
    return {result,os.clock() - SCRIPT_START_TIME}
end


local function redis_type(key)
    return redis.call('type', key)['ok']
end

-- The length of any structure in redis
local function redis_len(key)
    local command = type_table[redis_type(key)]
    if command then
    	return redis.call(command, key) + 0
    else
        return 0
    end
end

-- Create a unique random key
local function redis_randomkey(prefix)
    local rnd_key = prefix .. ':tmp:' .. math.random(1,100000000)
    if redis.call('exists', rnd_key) + 0 == 1 then
        return randomkey()
    else
        return rnd_key
    end
end

-- table of all members at key.
-- If the key is a string returns an empty table
-- If an argumnet is passed with value true all elements of the structure are returned.
local function redis_members(key, all, typ)
	if not typ then
		typ = redis.call('type',key)['ok']
	end
	if typ == 'set' then
		return redis.call('smembers', key)
	elseif typ == 'zset' then
		if all then
			return redis.call('zrange', key, 0, -1, 'withscores')
		else
			return redis.call('zrange', key, 0, -1)
		end
	elseif typ == 'list' then
		return redis.call('lrange', key, 0, -1)
	elseif typ == 'hash' then
		if all then
			return redis.call('hgetall', key)
		else
			return redis.call('hkeys', key)
		end
	elseif typ == 'ts' then
	    return timeseries.call('irange', key, 0, -1)
	else
		return {}
	end
end

-- delete keys from a table
local function redis_delete(keys)
	local n = table.getn(keys)
	if n > 0 then
		return redis.call('del', unpack(keys)) + 0
	end
	return n 
end
