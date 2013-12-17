if redis then
    -- THE FIRST ARGUMENT IS THE NAME OF THE SCRIPT
    if # ARGV < 1 then
        error('Wrong number of arguments.')
    end
    if # KEYS < 2 then
        error('Wrong number of keys.')
    end
    local destkey, key = KEYS[1], KEYS[2]
    local meta = cjson.decode(ARGV[1])
    local load_only
    local ids = redis.call('smembers', key)
    if destkey == key then
        redis.call('del', key)
    end
    if # ARGV == 2 then
        load_only = cjson.decode(ARGV[2])
    end
    
    local function setnumber(this, name, field)
        this[name] = field + 0
    end
    
    for _, id in ipairs(ids) do
        local okey = meta.namespace .. ':obj:' .. id
        local this = {{}}
        if load_only == nil then
            local fields = redis.call('hgetall', okey)
            local name = nil
            for _, field in ipairs(fields) do
                if name == nil then
                    name = field
                else
                    if pcall(setnumber, this, name, field) == false then
                        this[name] = field
                    end
                    name = nil
                end
            end
        else
            local fields = redis.call('hmget', okey, unpack(load_only))
            for i, field in ipairs(fields) do
                local name = load_only[i]
                if pcall(setnumber, this, name, field) == false then
                    this[name] = field
                end
            end
        end
        if {0[where_clause]} then
            redis.call('sadd', destkey, id)
        end
    end
end