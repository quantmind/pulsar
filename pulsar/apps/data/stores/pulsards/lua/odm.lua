--[[
Redis lua script for managing object-data mapping and queries. The script
define the odm namespace, where the pseudo-class odm.Model is the main component. 
--]]
local AUTO_ID, COMPOSITE_ID, CUSTOM_ID = 1, 2, 3
-- odm namespace - object-data mapping
local odm = {
    redis=nil,
    TMP_KEY_LENGTH = 12,
    ModelMeta = {
        namespace = '',
        id_type = AUTO_ID,
        id_name = 'id',
        id_fields = {},
        multi_fields = {},
        sorted = false,
        autoincr = false,
        indices = {}
    },
    range_selectors = {
        ge = function (v, v1)
            return v+0 >= v1+0
        end,
        gt = function (v, v1)
            return v+0 > v1+0
        end,
        le = function (v, v1)
            return v+0 <= v1+0
        end,
        lt = function (v, v1)
            return v+0 < v1+0
        end,
        startswith = function (v, v1)
            return string.sub(v, 1, string.len(v1)) == v1
        end,
        endswidth = function (v, v1)
            return string.sub(v, string.len(v) - string.len(v1) + 1) == v1
        end,
        contains = function (v, v1)
            return string.find(v, v1) ~= nil 
        end
    }
}
-- Model pseudo-class
odm.Model = {
    --[[
     Initialize model with model MetaData table
    --]]
    init = function (self, meta)
        self.meta = tabletools.json_clean(meta)
        self.idset = self.meta.namespace .. ':id'    -- key for set containing all ids
        self.auto_ids = self.meta.namespace .. ':ids' -- key for auto ids
        return self
    end,
    --[[
     Commit a session to redis
        num: number of instances to commit
        args: table containing instances data to save. The data is an array
            containing arrays of the form:
                {action, id, score, N, d_1, ..., d_N] 
        @return an array of id saved to the database
    --]]
    commit = function (self, num, args)
        local count, p, results = 0, 0, {}
        while count < num do
            local action, prev_id, id, score, idx0 = args[p+1], args[p+2], args[p+3], args[p+4], p+5
            local length_data = args[idx0] + 0
            local data = tabletools.slice(args, idx0+1, idx0+length_data)
            count = count + 1
            p = idx0 + length_data
            results[count] = self:_commit_instance(action, prev_id, id, score, data)
        end
        return results
    end,
    --[[
        Build a new query and store the resulting ids into destkey.
        It returns the size of the set in destkey.
        :param field: the field to query
        :param destkey: the key which will store the set of ids resulting from the query
        :param queries: an array containing pairs of query_type, value where query_type
            can be one of 'set', 'value' or a range filter.
    --]]
    query = function (self, destkey, field, queries)
        local ranges, unique, qtype, oper, nested = {}, self.meta.indices[field]
        for i, value in ipairs(queries) do
            if 2*math.floor(i/2) == i then
                if qtype == 'set' then
                    oper = true
                    self:_queryset(destkey, field, unique, value)
                elseif qtype == 'value' then
                    oper = true
                    self:_queryvalue(destkey, field, unique, value)
                else
                    -- Range queries are processed together
                    local selector = odm.range_selectors[qtype]
                    if selector then
                        value, nested = unpack(cjson.decode(value))
                        table.insert(ranges, {selector=selector, value=value, nested=nested})
                    else
                        error('Cannot understand query type "' .. qtype .. '".')
                    end
                end
            else
                qtype = value
            end
        end
        if # ranges > 0 then
            if not oper then
                self:_selectranges(destkey, self.idset, field, ranges)
            else
                self:_selectranges(destkey, destkey, field, ranges)
            end
        end
        return self:setsize(destkey)
    end,
    --[[
        Delete a query stored in key id
    --]]
    delete = function (self, key)
        local ids, results = redis_members(key), {}
        for _, id in ipairs(ids) do
            local idkey = self:object_key(id)
            self:_update_indices(false, id)
            local num = odm.redis.call('del', idkey) + 0
            self:remove_from_set(self.idset, id)
            if self.meta.multi_fields then
                for _, name in ipairs(self.meta.multi_fields) do
                    odm.redis.call('del', idkey .. ':' .. name)
                end
            end
            if num == 1 then
                table.insert(results, id)
            end
        end
        return results
    end,
    --[[
    --]]
    aggregate = function (self, destkey, field)
        -- Loop over ids to aggregate id from a recursive-related field
        local processed = {}
        for _, id in ipairs(self:setids(destkey)) do
            self:_aggregate(destkey, id, field, processed)
        end
    end,
    --[[
        Load instances from ids stored in a query temporary key
        :param key: the key containing the set of ids
        :param options: dictionary of options 
    --]]
    load = function (self, key, options)
        local result, ids, related_items
        options = tabletools.json_clean(options)
        if options.get and options.get ~= '' then
            return redis_members(key)
        elseif options.ordering == 'explicit' then
            ids = self:_explicit_ordering(key, options.start, options.stop, options.order)
        elseif options.ordering == 'DESC' then
            ids = odm.redis.call('zrevrange', key, options.start, options.stop)
        elseif options.ordering == 'ASC' then
            ids = odm.redis.call('zrange', key, options.start, options.stop)
        else
            ids = odm.redis.call('smembers', key)
        end
        -- Now load fields
        if options.fields and # options.fields > 0 then
            if # options.fields == 1 and options.fields[1] == self.meta.id_name then
                result = ids
            else
                result = {}
                for _, id in ipairs(ids) do
                    table.insert(result, {id, odm.redis.call('hmget', self:object_key(id), unpack(options.fields))})
                end
            end
        else
            result = {}
            for _, id in ipairs(ids) do
                table.insert(result, {id, odm.redis.call('hgetall', self:object_key(id))})
            end
        end
        if options.related then
            related_items = self:_load_related(result, options.related)
        else
            related_items = {}
        end
        return {result, related_items}
    end,
    --
    --          INTERNAL METHODS
    --
    object_key = function (self, id)
        return self.meta.namespace .. ':obj:' .. id
    end,
    --
    map_key = function (self, field)
        return self.meta.namespace .. ':uni:' .. field
    end,
    --
    index_key = function (self, field, value)
        local idxkey = self.meta.namespace .. ':idx:' .. field .. ':'
        if value then
            idxkey = idxkey .. value
        end
        return idxkey
    end,
    --
    --[[
        A temporary key in the model namespace
    --]]
    temp_key = function (self)
        local bk = self.meta.namespace .. ':tmp:'
        while true do
            local chars = {}
            for loop = 1, odm.TMP_KEY_LENGTH do
                chars[loop] = string.char(math.random(1, 255))
            end
            local key = bk .. table.concat(chars)
            if odm.redis.call('exists', key) + 0 == 0 then
                return key
            end
        end
    end,
    --
    setsize = function(self, setid)
        if self.meta.sorted then
            return odm.redis.call('zcard', setid)
        else
            return odm.redis.call('scard', setid)
        end
    end,
    --
    setids = function(self, setid)
        if self.meta.sorted then
            return odm.redis.call('zrange', setid, 0, -1)
        else
            return odm.redis.call('smembers', setid)
        end
    end,
    --
    setadd = function(self, setid, score, id, autoincr)
        if autoincr then
            score = odm.redis.call('zincrby', setid, score, id)
        elseif self.meta.sorted then
            odm.redis.call('zadd', setid, score, id)
        else
            odm.redis.call('sadd', setid, id)
        end
        return score
    end,
    --
    remove_from_set = function(self, setid, id)
        if self.meta.sorted then
            odm.redis.call('zrem', setid, id)
        else
            odm.redis.call('srem', setid, id)
        end
    end,
    --
    -- Check if an id is available in the setid
    has_id = function(self, id)
        if self.meta.sorted then
            return odm.redis.call('zscore', self.idset, id)
        else
            return odm.redis.call('sismember', self.idset, id) + 0 == 1
        end
    end,
    --
    _queryset = function(self, destkey, field, unique, key)
        if field == self.meta.id_name then
            self:_add_to_dest(destkey, field, key)
        elseif unique then
            local mapkey, ids = self:map_key(field), self:setids(key)
            for _, v in ipairs(ids) do
                add(odm.redis.call('hget', mapkey, v))
            end
        elseif unique == false then
            self:_add_to_dest(destkey, field, key, true)
        else
            error('Cannot query on field "' .. field .. '". Not an index.')
        end 
    end,
    --
    _queryvalue = function(self, destkey, field, unique, value)
        if field == self.meta.id_name then
            self:_add(destkey, field, value)
        elseif unique then
            local mapkey = self:map_key(field)
            self:_add(destkey, field, odm.redis.call('hget', mapkey, value))
        elseif unique == false then
            self:_union(destkey, field, value)
        else
            error('Cannot query on field "' .. field .. '". Not an index.')
        end
    end,
    --
    _add = function(self, destkey, field, id)
        -- field is not used, but is here to have the same signature as _union
        if id then
            if self.meta.sorted then
                local score = redis.call('zscore', self.idset, id)
                if score then
                    redis.call('zadd', destkey, score, id)
                end
            else
                if redis.call('sismember', self.idset, id) + 0 == 1 then
                    redis.call('sadd', destkey, id)
                end
            end
        end
    end,
    --
    _union = function(self, destkey, field, value)
        local idxkey = self:index_key(field, value)
        if self.meta.sorted then
            odm.redis.call('zunionstore', destkey, 2, destkey, idxkey)
        else
            odm.redis.call('sunionstore', destkey, destkey, idxkey)
        end
    end,
    --
    _add_to_dest = function(self, destkey, field, key, as_union)
        local processed = {}
        for _, id in ipairs(redis_members(key)) do
            if not processed[id] then
                if as_union then
                    self:_union(destkey, field, id)
                else
                    self:_add(destkey, field, id)
                end
                processed[id] = true
            end
        end
    end,
    --
    _selectranges = function(self, destkey, fromkey, field, ranges)
        local ordered, ids, scores, value, key, status = self.meta.sorted
        if ordered then
            ids, scores = {}, {}
            for i, score in ipairs(self.redis.call('zrange', fromkey, 0, -1, 'withscores')) do
                if 2*math.floor(i/2) == i then
                    table.insert(scores, score)
                else
                    table.insert(ids, score)
                end
            end
        else
            ids = redis.call('smembers', fromkey)
        end
        redis.call('del', destkey)
        if field ~= self.meta.id_name then
            for _, range in ipairs(ranges) do
                table.insert(range.nested, field)
            end
        end
        -- loop over ids to perform range selection
        for i, id in ipairs(ids) do
            -- loop through range selectors
            for _, range in ipairs(ranges) do
                if # range.nested > 0 then
                    _, value = self:_nested_field(id, range.nested)
                else
                    value = id
                end
                if not (value and range.selector(value, range.value)) then
                    value = nil
                    break
                end
            end
            if value then
                if ordered then
                    redis.call('zadd', destkey, scores[i], id)
                else
                    redis.call('sadd', destkey, id)
                end
            end
        end
    end,
    --
    _commit_instance = function (self, action, prev_id, id, score, data)
        -- Commit one instance and update indices
        local created_id, errors = false, {}
        if self.meta.id_type == AUTO_ID then
            if id == '' then
                created_id = true
                id = odm.redis.call('incr', self.auto_ids)
            else
                id = id + 0 --  must be numeric
                local counter = odm.redis.call('get', self.auto_ids)
                if not counter or counter + 0 < id then
                    odm.redis.call('set', self.auto_ids, id)
                end
            end
        end
        if id == '' then
            table.insert(errors, 'Id not available. Cannot commit.')
        else
        	-- If no previous ID force the action to be add
        	if prev_id == '' then
        		prev_id = id
        		action = 'add'
        	end
            local idkey, original_data, field = self:object_key(prev_id), {}
            if action ~= 'add' then  -- override or update
                original_data = odm.redis.call('hgetall', idkey)
                -- remove indices
                self:_update_indices(false, prev_id)
                -- when overriding, remove all data from previous hash table
                -- only if the previous id is the same as the current one.
                if action == 'override' and prev_id .. '' == id .. '' then
                    odm.redis.call('del', idkey)
                end
            end
            -- remove previous id from the set of ids
            if id ~= prev_id then
            	idkey = self:object_key(id)
                self:remove_from_set(self.idset, prev_id)
            end
            -- Add id to the idset
            score = self:setadd(self.idset, score, id, self.meta.autoincr)
            -- set the new data in the hash table
            if # data > 0 then
                odm.redis.call('hmset', idkey, unpack(data))
            end
            errors = self:_update_indices(true, id, prev_id, score)
            -- An error has occurred. Rollback changes.
            if # errors > 0 then
                -- Remove indices
                self:_update_indices(false, id)
                if action == 'add' then
                    self:remove_from_set(self.idset, id)
                    if created_id then
                        odm.redis.call('decr', self.auto_ids)
                        id = ''
                    end
                elseif # original_data > 0 then
                    id = prev_id
                    idkey = self:object_key(id)
                    odm.redis.call('hmset', idkey, unpack(original_data))
                    self:_update_indices(true, id, prev_id, score)
                end
            end
        end
        if # errors > 0 then
            return {id, 0, errors[1]}
        else
            return {id, 1, score}
        end
    end,
    --
    _update_indices = function (self, update, id, oldid, score)
        local idkey, errors, idxkey, value = self:object_key(id), {}
        for field, unique in pairs(self.meta.indices) do
            -- obtain the field value
            value = odm.redis.call('hget', idkey, field)
            if unique then
                idxkey = self:map_key(field) -- id for the hash table mapping field value to instance ids
                if update then
                    -- Check if the unique field is already available
                    if odm.redis.call('hsetnx', idxkey, value, id) + 0 == 0 then
                        -- The value was already available! If the oldid is different from current id and the
                        -- index match the oldid, it is fine otherwise it is a conflict
                        local stored_id = odm.redis.call('hget', idxkey, value)
                        if oldid == id or not stored_id == oldid then
                        	-- check that the stored_id actually exists!
                            if self:has_id(stored_id) then
	                            -- remove the field from the instance hashtable so that
	                            -- the next call to _update_indices won't delete the index. Important!
	                            odm.redis.call('hdel', idkey, field)
	                            table.insert(errors, 'Unique constraint "' .. field .. '" violated: "' .. value .. '" is already in database.')
	                        else
                                odm.redis.call('hset', idxkey, value, id)
                            end
                        end
                    end
                elseif value then
                    odm.redis.call('hdel', idxkey, value)
                end
            else
                idxkey = self:index_key(field, value)
                if update then
                    self:setadd(idxkey, score, id)
                else
                    self:remove_from_set(idxkey, id)
                end
            end
        end
        return errors
    end,
    --
    -- Perform explicit ordering via redis SORT command.
    _explicit_ordering = function (self, key, start, stop, order)
        local okey, tkeys, sortargs, bykey, ids, status = key, {}, {}
        -- nested sorting for foreign key fields
        if order.nested and # order.nested > 0 then
            -- generate a temporary key where to store the hash table holding
            -- the values to sort with
            local skey = self:temp_key()
            for i, id in ipairs(redis_members(key)) do
                local value, key = redis.call('hget', self:object_key(id), order.field)
                for n, name in ipairs(order.nested) do
                    if 2*math.floor(n/2) == n then
                        value = redis.call('hget', key, name)
                    else
                        -- Check test_sort_by_missing_fk_data test if fknotrequired tests
                        status, key = pcall(function() return name .. ':obj:' .. value end)
                        if status == false then
                             key = okey
                             break
                        end
                    end
                end
                -- store value on temporary key
                tkeys[i] = skey .. id
                redis.call('set', tkeys[i], value)
            end
            bykey = skey .. '*'
        elseif order.field ~= '' then
            bykey = self:object_key('*->' .. order.field)
        end
        -- sort by field
        if bykey then
           sortargs = {'BY', bykey}
        end
        if start > 0 or stop > 0 then
            table.insert(sortargs, 'LIMIT')
            table.insert(sortargs, start)
            table.insert(sortargs, stop)
        end
        if order.method == 'ALPHA' then
            table.insert(sortargs, 'ALPHA')
        end
        if order.desc then
            table.insert(sortargs, 'DESC')
        end
        ids = odm.redis.call('sort', key, unpack(sortargs))
        redis_delete(tkeys)
        return ids
    end,
    --
    -- Load related objects with their fields
    _load_related = function (self, result, related)
        local related_items = {}
        for name, rel in pairs(related) do
            local field_items, field, fields = {}, rel.field, rel.fields
            table.insert(related_items, {name, field_items, rel.fields})
            -- A structure has type defined
            if # rel.type > 0 then
                for i, res in ipairs(result) do
                    local id = res[1]
                    local fid = self:object_key(id .. ':' .. field)
                    field_items[i] = {id, redis_members(fid, true, rel.type)}
                end
            -- A Foreign Key
            else
                local rbk, processed = rel.bk, {}
                for i, res in ipairs(result) do
                    local rid = redis.call('hget', self:object_key(res[1]), field)
                    if rid then
                        local val = processed[rid]
                        -- The related field needs to be loaded
                        if not val then
                            local related_key = rbk .. ':obj:' .. rid
                            val = 1
                            if redis_type(related_key) == 'hash' then
                                if # fields == 1 and fields[1] == '' then
                                    table.insert(field_items, rid)
                                else
                                    if # fields > 0 then
                                        val = redis.call('hmget', related_key, unpack(fields))
                                    else
                                        val = redis.call('hgetall', related_key)
                                    end
                                    table.insert(field_items, {rid, val})
                                end
                            end
                            --elseif # fields == 0 then
                            --	-- There are no fields for this related model.
                            --	-- A corner case for which there few tests.
                            --	val = {}
                            --	table.insert(field_items, {rid, val})
                            --end
                            processed[rid] = val
                        end
                    end
                end
            end
        end
        return related_items
    end,
    -- Aggregate ids into destkey
    _aggregate = function (self, destkey, id, field, processed)
        if not processed[id] then
            processed[id] = true
            for _, rid in ipairs(self:setids(self:index_key(field, id))) do
                self:_add(destkey, field, rid)
                self:_aggregate(destkey, rid, field, processed)
            end
        end
    end,
    --
    _nested_field = function (self, id, nested)
        local key, status, value = self:object_key(id)
        for n, field_model_name in ipairs(nested) do
            if 2*math.floor(n/2) < n then
                -- odd elements we get the value
                value = redis.call('hget', key, field_model_name)
            else    -- even we get the next key
                status, key = pcall(function() return field_model_name .. ':obj:' .. value end)
                if not status then
                    key=  nil
                    value = nil
                    break
                end
            end
        end
        return key, value
    end
}
--
-- Constructor
function odm.model(meta)
    return odm.Model:init(meta)
end
-- Return the module only when this module is not in REDIS
if not redis then
    return odm
else
    odm.redis = redis
    local function first_key(keys)
        if # keys > 0 then
            return keys[1]
        else
            error('Script query requires 1 key for the id set')
        end
    end
    -- MANAGE ALL MODEL SCRIPTS called by stdnet
    local scripts = {
        -- Commit a session to redis
        commit = function(self, model, keys, num, args)
            return model:commit(num+0, args)
        end,
        -- Build a query and store results on a new set. Returns the set id
        query = function(self, model, keys, field, args)
            return model:query(first_key(keys), field, args)
        end,
        -- Load a query
        load = function(self, model, keys, options, args)
            return model:load(first_key(keys), cjson.decode(options))
        end,
        -- delete a query
        delete = function(self, model, keys, ...)
            return model:delete(first_key(keys))
        end,
        -- recursively add id to a set
        aggregate = function(self, model, keys, field, args)
            return model:aggregate(first_key(keys), field)
        end,
        -- structure. Don nothing
        structure = function(self, model, keys, ...)
            return ''
        end
    }
    -- THE FIRST ARGUMENT IS THE NAME OF THE SCRIPT
    if # ARGV < 2 then
        error('Wrong number of arguments.')
    end
    local script, meta, arg, args = scripts[ARGV[1]], cjson.decode(ARGV[2]) 
    if not script then
        error('Script ' .. ARGV[1] .. ' not available')
    end
    if # ARGV > 2 then
        arg = ARGV[3]
        args = tabletools.slice(ARGV, 4, -1)
    end
    return script(scripts, odm.model(meta), KEYS, arg, args)
end
