local tabletools = {}

-- Initialize an array of size *size* fill with *value*
tabletools.init = function (size, value)
    local vector = {}
    for i = 1, size do
        vector[i] = value
    end
    return vector
end

-- Check if two arrays are equals
tabletools.equal = function (v1, v2)
    if # v1 == # v2 then
        for i, v in ipairs(v1) do
            if v ~= v2[i] then
                return false
            end
        end
        return true
    else
        return false
    end
end

-- Slice a lua table between i1 and i2
tabletools.slice = function (values, i1, i2)
    local res = {}
    local n = #values
    -- default values for range
    i1 = i1 or 1
    i2 = i2 or n
    if i2 < 0 then
        i2 = n + i2 + 1
    elseif i2 > n then
        i2 = n
    end
    if i1 < 1 or i1 > n then
        return {}
    end
    local k = 1
    for i = i1,i2 do
        res[k] = values[i]
        k = k + 1
    end
    return res
end

-- Convert a dictionary into a flat array. For example {bla = 'foo', planet = 'mars'}
-- becomes {'bla', 'foo', 'planet', 'mars'}
tabletools.flat = function (tbl)
    local result = {}
    for name,value in pairs(tbl) do
        table.insert(result,name)
        table.insert(result,value)
    end
    return result
end

tabletools.asdict = function (tbl)
    local result, key = {}
    for i, value in ipairs(tbl) do
        if 2*math.floor(i/2) == i then
            result[key] = value
        else
            key = value
        end
    end
    return result
end

tabletools.load_code = function(code, environment)
    if setfenv and loadstring then
        local f = assert(loadstring(code))
        setfenv(f, environment)
        return f
    else
        return assert(load(code, nil,"t",environment))
    end
end

tabletools.json_clean = function (meta)
    local m, t = {}
    for k, v in pairs(meta) do
        t = type(v)
        -- json return null as a function while cjson as userdata. In both
        -- cases we don't want the values.
        if t ~= 'function' and t ~= 'userdata' then
            if t == 'table' then
                v = tabletools.json_clean(v)
            end
            m[k] = v
        end
    end
    return m
end

-- Return the module only when this module is not in REDIS
if not (KEYS and ARGV) then
    return tabletools
end
