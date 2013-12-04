-- Not a number
local nan = 0/0
local NaN = 'NaN'
-- 9 bytes string for nil data
local nildata = string.char(0,0,0,0,0,0,0,0,0)


-- Column timeseries class
local columnts = {
    --
    -- Initialize
    init = function (self, key)
        self.key = key
        self.fieldskey = key .. ':fields'
    end,
    --
    -- field data key
    fieldkey = function (self, field)
        return self.key .. ':field:' .. field
    end,
    --
    -- all field names for this timeseries
    fields = function (self)
        return redis.call('smembers', self.fieldskey)
    end,
    --
    -- num fields
    num_fields = function (self)
        return redis.call('scard', self.fieldskey) + 0
    end,
    --
    -- Returns a dictionary of field names/field keys
    fields_set = function(self)
        local f = {}
        for _,name in ipairs(self:fields()) do
            f[name] = self:fieldkey(name)
        end
        return f
    end,
    --
    -- Length of timeseries
    length = function (self)
        return redis.call('zcard', self.key) + 0
    end,
    --
    -- Delete timeseries
    del = function(self)
        local keys = redis.call('keys', self.key .. '*')
        if # keys > 0 then
            redis.call('del', unpack(keys))
        end
    end,
    --
    -- Return the ordered list of times
    times = function (self, ...)
    	if # arg == 2 then
    		local start = arg[1] + 0
    		local stop = arg[2] + 0
    		return redis.call('zrangebyscore', self.key, start, stop)
    	elseif # arg == 0 then
        	return redis.call('zrange', self.key, 0, -1)
        else
        	error('times accept 0 or two arguments')
        end
    end,
    --
    itimes = function (self, start, stop)
        return redis.call('zrange', self.key, start, stop)
    end,
    --
    -- The rank of timestamp in the timeseries
    exists = function (self, timestamp)
        if redis.call('zrank', self.key, timestamp+0) then
        	return true
        else
        	return false
        end
    end,
    --
    -- The rank of timestamp in the timeseries
    rank = function (self, timestamp)
        return redis.call('zrank', self.key, timestamp+0)
    end,
    --
    -- Get all field values at timestamp. Return a dictionary of values or nil
    get = function (self, timestamp, skip_unpack)
    	local dv = self:range(timestamp, timestamp, nil, skip_unpack)
    	return self:_single_field_dict(dv)
    end,
    --
    -- remove a timestamp from timeseries and return it
    pop = function(self, timestamp, skip_unpack)
        local rank = redis.call('zrank', self.key, timestamp+0)
        return self:ipop(rank, skip_unpack)
    end,
    --
    ipop = function(self, index, skip_unpack)
    	if index then
    		local data = self:ipop_range(index, index, skip_unpack)
    		return self:_single_field_dict(data)
        end
    end,
    --
    -- remove a field and return true or false
    popfield = function (field)
        return redis.call('del', self:fieldkey(field))['ok'] + 0 == 1
    end,
    --
    -- Return the unpacked value of field at rank
    rank_value = function (self, rank, field)
    	if rank < 0 then
    		rank = self:length() + rank
    	end
        local r = 9*rank
        local value = redis.call('getrange',self:fieldkey(field),r,r+9)
        return self:unpack_value(value)
    end,
    --
    -- shortcut for returning the whole range of a timeserie
    all = function (self, fields, skip_unpack)
        return self:irange(0, -1, fields, skip_unpack)
    end,
    --
    -- A representation of the timeseries as a dictionary.
    -- If only one field is available, values will be the field values
    -- otherwise it will be a dictionary of fields.
    -- *Values are unpacked*
    asdict = function (self, fields)
        if self:length() == 0 then
            return nil
        end
        local times, field_values = unpack(self:all(fields))
        local result = {}
        local field_name
        local count = 0
        for fname, field in pairs(field_values) do
            count = count + 1
            field_name = fname
        end
        if count == 1 then
            field_values = field_values[field_name]
            for i,time in ipairs(times) do
                result[time] = field_values[i]
            end
        else
            for i,time in ipairs(times) do
                fvalues = {}
                for fname,field in pairs(field_names) do
                    fvalues[fname] = field_values[fname][i]
                end
                result[time] = fvalues
            end
        end
        return result
    end,
    --
    -- Add a timeseries *ts*, multiplied by the given weight, to self
    addserie = function (self, ts, weight, fields, tsmul)
    	-- get data without unpacking. IMPORTANT
        local times, field_values = unpack(ts:all(fields))
        return self:add(times, field_values, weight, tsmul)
    end,
    --
    -- remove a range by time from the timeseries and return it
    pop_range = function(self, start, stop, skip_unpack)
        local i1 = self:rank(start)
        local i2 = self:rank(stop)
        return self:ipop_range(i1, i2, skip_unpack)
    end,
    --
    -- remove a range by rank from the timeseries and returns it
    ipop_range = function(self, i1, i2, skip_unpack)
    	local length = self:length()
    	if i1 and i2 and length > 0 then
    		i1 = i1 + 0
    		if i1 < 0 then
    			i1 = math.max(length + i1, 0)
    		end
    		local times = self:itimes(i1, i2)
    		local len = # times
    		if len == 0 then
    			return {{}, {}}
    		end
    		assert(redis.call('zremrangebyrank', self.key, i1, i2)+0 == len,
    			 	'Critical error while removing range')
    		local i2 = i1 + len
    		local fields = self:fields()
    		local field_values = {}
    		local data = {times, field_values}
    		for i, field in ipairs(fields) do
                local fkey = self:fieldkey(field)
                local removed_data = redis.call('getrange', fkey, i1*9, i2*9-1)
                local new_data = ''
                if i1 > 0 then
                	new_data = redis.call('getrange', fkey, 0, i1*9-1)
                end
                if i2 < length then
                	new_data = new_data .. redis.call('getrange', fkey, i2*9, -1)
                end
                field_values[field] = self:fill_table(removed_data, len, skip_unpack)
                redis.call('set', fkey, new_data)
            end
            return data
        else
        	return {{}, {}}
        end
    end,
    --
    range = function (self, start, stop, fields, skip_unpack)
    	local times = self:times(start, stop)
    	return self:get_range(times, fields, skip_unpack)
    end,
    --
    -- return a range by rank (index)
    irange = function (self, start, stop, fields, skip_unpack)
    	local times = self:itimes(start, stop)
    	return self:get_range(times, fields, skip_unpack)
    end,
    --
    -- return an array containg a range of the timeseries. The array
    -- contains two elements, an array of times and a dictionary of fields.
    get_range = function (self, times, fields, skip_unpack)
        local field_values = {}
        local data = {times, field_values}
        local len = # times
        if len == 0 then
            return data
        end
        -- get the start rank (Also when we use tsrange. Important)
        local start = self:rank(times[1])
        local stop = start + len
        if not fields or # fields == 0 then
            fields = self:fields()
        end
        -- loop over fields
        for i, field in ipairs(fields) do
            local fkey = self:fieldkey(field)
            if redis.call('exists', fkey) + 0 == 1 then
                -- Get the string between start and stop from redis
                local sdata = redis.call('getrange', fkey, 9*start, 9*stop)
                field_values[field] = self:fill_table(sdata, len, skip_unpack)
            end
        end
        return data
    end,
    --
    -- LOW LEVEL FUNCTION. DO NOT USE DIRECTLY
    -- Add/replace field values. If weights are provided, the values in
    -- field_values MUST NOT BE UNPACKED and they are added to existing
    -- values, otherwise the values are to be replaced. tsmul is an optional
    -- single field timeseries which multiply each field in self.
    --
    -- times is a table containing times
    -- field_values is a dictionary of field-values where values have the same
    -- 		length as *times*
    add = function (self, times, field_values, weights, tsmul)
        local fields = self:fields_set()
        local tslen = self:length() + 0
        local ws = {}
        local fkey, data, rank, rank9, available, weight, value, dvalue, v1, mul
        local new_serie = tslen == 0
        local time_set = {}
        if tsmul then
            assert(tsmul:length() > 0, 'Timeseries ' .. tsmul.key .. ' not available')
            assert(tsmul:num_fields() == 1, 'Timeseries ' .. tsmul.key .. ' has more than one field. Cannot be used tu multiply timeseries.')
            tsmul = tsmul:asdict()
        end
        -- Make sure all fields are available and have same data length
        for field, value in pairs(field_values) do
            -- add field to the fields set
            if not fields[field] then
            	fkey = self:fieldkey(field)
                redis.call('sadd', self.fieldskey, field)
                fields[field] = fkey
                if tslen > 0 then
                    redis.call('set', fkey, string.rep(nildata, tslen))
                end
            end
        end
        -- If we are adding to an existing timeseries
        -- Fill the time_set dictionary with false values at self:times
        if weights and not new_serie then
            local times = self:times()
            for index, timestamp in ipairs(times) do
                time_set[timestamp] = false
            end
        end
        -- Loop over times
        for index, timestamp in ipairs(times) do
            time_set[timestamp] = true
            available = self:rank(timestamp)
            -- This is a new timestamp
            if not available then
                redis.call('zadd', self.key, timestamp, timestamp)
                rank = redis.call('zrank', self.key, timestamp) + 0
                rank9 = 9*rank
                tslen = self:length()
                -- loop over all fields and append/insert new data to the field strings
                for field, fkey in pairs(fields) do
                    -- not at the end of the string! inefficient insertion.
                    if rank < tslen - 1 then
                        data = nildata .. redis.call('getrange', fkey, rank9, -1)
                    else
                        data = nildata
                    end
                    redis.call('setrange', fkey, rank9, data)
                end
            -- No need to insert a new timestamp
            else
                rank = available + 0
                rank9 = 9*rank
            end

            -- Loop over field/values pairs
	        for field, values in pairs(field_values) do
	            -- add field to the fields set
	            fkey = fields[field]
	            dvalue = tonumber(values[index])
	            if dvalue == nil or dvalue ~= dvalue then
	                value = nildata
	            else
    	            -- set the field value
    	            if weights then
    	                if type(weights) == 'number' then
    	                    weight = weights
    	                else
    	                    weight = weights[field]
    	                end
    	                dvalue = weight*dvalue
    	                if tsmul then
    	                   mul = tsmul[timestamp]
    	                   if mul then
    	                       dvalue = mul*dvalue
    	                   else
    	                       dvalue = nan
    	                   end
    	                end
    	                -- If the value is a number
    	                if dvalue == dvalue and not new_serie then
    	                    v1 = redis.call('getrange', fkey, rank9, rank9+9)
    	                    dvalue = dvalue + self:unpack_value(v1)
    	                end
    	            end
    	            value = self:pack_value(dvalue)
    	        end
	            redis.call('setrange', fkey, rank9, value)
	        end
	    end

	    if weight then
	        for timestamp, avail in pairs(time_set) do
	            if not avail then
	               rank9 = redis.call('zrank', self.key, timestamp)*9
	               for field, fkey in pairs(fields) do
                        redis.call('setrange', fkey, rank9, nildata)
	               end
	           end
	       end
	    end
    end,
    -- Information about the timeserie
    info = function (self, start, stop, field_values)
        local times, field_values = unpack(self:all())
        local size = # times
        local result = {size=size, fields={}}
        if size > 0 then
            result['start'] = times[1]
            result['stop'] = times[size]
        end
        if size > 0 then
            for field, data in pairs(field_values) do
                local f = {}
                local missing = 0
                for _, v in ipairs(data) do
                    if v ~= v then
                        missing = missing + 1
                    end
                end
                result.fields[field] = {missing=missing}
            end
        end
        return result
    end,
    --
    ----------------------------------------------------------------------------
    -- INTERNAL FUNCTIONS
    ----------------------------------------------------------------------------
    --
    -- unpack a single value
    unpack_value = function (self, value)
        local byte = string.byte(value)
        assert(string.len(value) >= 9, 'Invalid string to unpack. Length is ' .. string.len(value))
        if byte == 1 then
            return struct.unpack('>i', string.sub(value,2,5))
        elseif byte == 2 then
            return struct.unpack('>d', string.sub(value,2))
        else
            return nan
        end
    end,
    --
    -- unpack to JSON serializable value
    unpack_value_s = function (self, value)
        local v = self:unpack_value(value)
        if v ~= v then
            return NaN
        else
            return v
        end
    end,
    --
    -- pack a single value
    pack_value = function (self, value)
        if value == value then
            return string.char(2) .. struct.pack('>d', value)
        else
            return nildata
        end
    end,
    --
    -- unpack values at a given index, it returns a dictionary
    -- of field - unpacked values pairs
    unpack_values = function (self, index, field_values)
        fields = {}
        for field,values in pairs(field_values) do
            local idx = 9*index
            local value = string.sub(values,idx+1,idx+9)
            fields[field] = self:unkpack_value(value)
        end
        return fields
    end,
    --
    -- string value for key
    string_value = function (self, key)
        return {key,redis.call('get', self.key .. ':key:' .. key)}
    end,
    --
    -- fill table with values
    fill_table = function(self, sdata, len, serializable)
    	local fdata = {}
        local p = 0
        local v
        if serializable then
        	while p < 9*len do
        	    v = self:unpack_value_s(string.sub(sdata, p+1, p+9))
                table.insert(fdata, v)
                p = p + 9
            end
        else
            while p < 9*len do
                v = self:unpack_value(string.sub(sdata, p+1, p+9))
                table.insert(fdata, v)
                p = p + 9
            end
        end
        return fdata
    end,
    --
    _single_field_dict = function(self, dv)
    	if # dv[1] == 1 then
    		local fields = {}
    		for field, data in pairs(dv[2]) do
    			fields[field] = data[1]
    		end
    		return fields
    	end
    end
}

local columnts_meta = {}
-- Constructor
function columnts:new(key)
    local result = {}
    if type(key) == 'table' then
        for i,k in ipairs(key) do
            result[i] = columnts:new(k)
        end
        return result
    elseif type(key) == 'number' then
        return key + 0
    else
        for k,v in pairs(columnts) do
            result[k] = v
        end
        result:init(key)
        return setmetatable(result, columnts_meta)
    end
end


--
-- merge timeseries
-- elements: an array of dictionaries containing the weight and an array of timeseries to multiply
-- fields: list of fiedls to merge
-- Multiply timeseries. At the moment this only works for two timeseries
-- ts1*ts2, with ts1 being a one field timeseries and ts2 being and N-fields timeseries.
-- It multiplies the field in ts1 for each fields in ts2 and store the result at key
-- with fields names given by ts2.
function columnts:merge(key, elements, fields)
    local result = columnts:new(key)
    result:del()
    -- First we copy the first timeseries across to self
    for i,elem in ipairs(elements) do
        assert( # elem.series <= 2, 'Too many timeseries. Cannot perform operation')
        -- More than one timeseries. Create the timeseries obtain by multiplying them
        local ts,tsmul = elem.series[1]
        if # elem.series == 2 then
            tsmul = elem.series[1]
            ts = elem.series[2]
        end
        result:addserie(ts, elem.weight, fields, tsmul)
    end
    return result
end


-- Return the module only when this module is not in REDIS
if not (KEYS and ARGV) then
    return columnts
end