-- UNIVARIATE TIMESERIES IN REDIS
--
-- Stand-alone Lua script for managing an univariate timeseries in Redis
--
-- A timeseries is an
--  1) ordered (with respect timestamps)
--  2) unique (each timestamp is unique within the timeseries)
--  3) associative (it associate a timestamp with a value)
-- container.
-- This script can be used by any clients, not just python-stdnet.
-- Commands are implemented in the *timeseries_commands* table. To
-- execute a command use the *timeseries* functions *call* and *call_multi*
--
-- timeseries.call('size', id)
-- timeseries.call('add', id, 100000, 'foo')
local function single_id(ids, cname)
    if # ids == 1 then
        return ids[1]
    else
        error('Only one timeseries permitted for command ' .. cname)
    end
end

-- COMMANDS TABLE
local timeseries_commands = {
    size = function (self, ids)
        local id = single_id(ids, 'len')
        return redis.call('zcard', id)
    end,
    -- Add timestamp-value pairs to the timeseries
    add = function (self, ids, ...)
        local id = single_id(ids, 'add')
        local N = # arg
        local h = math.floor(N/2)
        assert(2*h == N, 'Cannot add to timeseries. Number of arguments must be even')
        local i = 0
        local timestamp, value, stimestamp
        while i < N do
            timestamp = arg[i+1] + 0
            -- This is the trick. The value is a JSON encoded representation of
            -- a two-elements table given by the timestamp and the value itself
            value = cjson.encode({timestamp, arg[i+2]})
            -- This make sure of timestamp to be unique
            redis.call('zremrangebyscore', id, timestamp, timestamp)
            redis.call('zadd', id, timestamp, value)
            i = i + 2
        end
    end,
    -- Check if *timestamp* exists in the timeseries
    exists = function (self, ids, timestamp)
        local value = self._single_value(ids, timestamp, 'exists')
        if value then
            return true
        else
            return false
        end
    end,
    -- Rank of *timestamp* in timeseries
    rank = function (self, ids, timestamp)
        local value = self._single_value(ids, timestamp, 'rank')
        if value then
            local id = single_id(ids, 'rank')
            return redis.call('zrank', id, value)
        end
    end,
    -- Get the value associated with *timestamp*
    get = function (self, ids, timestamp)
        local value = self._single_value(ids, timestamp, 'get')
        if value then
            return cjson.decode(value)[2]
        end
    end,
    -- Remove and return the value associated with *timestamp* 
    pop = function(self, ids, timestamp)
        local value = self._single_value(ids, timestamp, 'pop')
        if value then
            local id = single_id(ids, 'pop')
            assert(redis.call('zrem', id, value)+0==1,
                    'Critical error while removing timestamp from timeseries')
            return cjson.decode(value)[2]
        end
    end,
    -- Remove and return the value at *index* 
    ipop = function(self, ids, index)
        local id = single_id(ids, 'ipop')
        local value = redis.call('zrange', id, index, index)
        if # value == 1 then
            value = value[1]
            assert(redis.call('zrem', id, value)+0==1,
                    'Critical error while removing timestamp from timeseries')
            return cjson.decode(value)[2]
        end
    end,
    -- list of timestamps between *timestamp1* and *timestamp2*
    times = function (self, ids, timestamp1, timestamp2)
        local id = single_id(ids, 'times')
        local range = redis.call('zrangebyscore', id, timestamp1, timestamp2)
        return self._get_times(range)
    end,
    -- list of timestamps between index *start* and *stop*
    itimes = function (self, ids, start, stop)
        local id = single_id(ids, 'itimes')
        local range = redis.call('zrange', id, start, stop)
        return self._get_times(range)
    end,
    -- The list of timestamp-value pairs between *timestamp1* and *timestamp2*
    range = function (self, ids, timestamp1, timestamp2)
    	local id = single_id(ids, 'range')
    	local range = redis.call('zrangebyscore', id, timestamp1, timestamp2)
    	return self._get_range(range)
    end,
    -- The list of timestamp-value pairs between index *start* and *stop*
    irange = function (self, ids, start, stop)
        local id = single_id(ids, 'irange')
        local range = redis.call('zrange', id, start, stop)
        return self._get_range(range)
    end,
    -- Count the number of elements between *timestamp1* and *timestamp2*
    count = function (self, ids, timestamp1, timestamp2)
        local id = single_id(ids, 'count')
        return redis.call('zcount', id, timestamp1, timestamp2)
    end,
    -- Remove and return a range between *timestamp1* and *timestamp2*
    pop_range = function(self, ids, timestamp1, timestamp2)
        local range = self.range(self, ids, timestamp1, timestamp2)
        if range then
            local id = single_id(ids, 'range')
            redis.call('zremrangebyscore', id, timestamp1, timestamp2)
            return range
        end
    end,
    -- Remove and return a range between index *start* and *stop*
    ipop_range = function(self, ids, start, stop)
        local range = self.irange(self, ids, start, stop)
        if range then
            local id = single_id(ids, 'ipop_range')
            redis.call('zremrangebyrank', id, start, stop)
            return range
        end
    end,
    --
    --	INTERNAL FUNCTIONS
    _single_value = function(ids, timestamp, name)
        local id = single_id(ids, name)
        local ra = redis.call('zrangebyscore', id, timestamp, timestamp)
        if # ra == 1 then
            return ra[1]
        elseif # ra > 1 then
            error('Critical error in timeserie. Multiple values for a timestamp')
        end
    end,
    --
    _get_times = function(range)
        local result = {}
        for i, value in ipairs(range) do
            table.insert(result, cjson.decode(value)[1])
        end
        return result
    end,
    --
    _get_range = function(range)
        local result = {}
        for i, value in ipairs(range) do
            table.insert(result, cjson.decode(value))
        end
        return result
    end,
}

-- Similar to redis global variable
local timeseries = {
    call = function(command, id, ...)
        local cmd = timeseries_commands[command]
        if not cmd then
            error('Timeserie command ' .. command .. ' not available')
        end
        return cmd(timeseries_commands, {id}, unpack(arg))
    end,
    --
    call_mult = function(command, ids, ...)
        local cmd = timeseries_commands[command]
        if not cmd then
            error('Timeserie command ' .. command .. ' not available')
        end
        return cmd(timeseries_commands, ids, unpack(arg))
    end
}