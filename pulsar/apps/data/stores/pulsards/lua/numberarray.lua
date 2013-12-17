-- An array of numbers for redis-lua

-- Not a number
local nan = 0/0
-- 8 bytes string for nil data
local nildata = string.char(0,0,0,0,0,0,0,0)

local array = {
    --
    -- Initialize with key and optional initial size and value
    init = function (self, key, size, value)
        self.key = key
        self:resize(size, value)
    end,
    -- length of array
    length = function (self)
        return (redis.call('strlen', self.key) + 0)/8
    end,
    -- Allocate extra size for the array
    resize = function (self, size, value)
        if size then
            size = size + 0
            local length = self:length()
            if size > length then
                if value then
                    value = self:pack(value)
                else
                    value = nildata
                end
                value = string.rep(value,size-length) 
                redis.call('setrange', self.key, 8*length, value)
            end
        end
        return self:length()
    end,
    --
    get = function (self, index, packed)
        index = index + 0
        assert(index > 0 and index <= self:length(),"Out of bound.")
        local start = 8*(index - 1)
        local val = redis.call('getrange', self.key, start, start+7)
        if packed then
            return val
        else
            return self:unpack(val)
        end
    end,
    set = function(self, index, value, packed)
        index = index + 0
        assert(index > 0 and index <= self:length(),"Out of bound.")
        local start = 8*(index - 1)
        if packed then
            value = self:pack(value)
        end
        return redis.call('setrange', self.key, start, value)
    end,
    --
    -- push_back
    push_back = function(self, value, packed)
        local start = 8*self:length()
        if not packed then
            value = self:pack(value)
        end
        redis.call('setrange', self.key, start, value)
    end,
    --
    all_raw = function(self)
    	local start
        local data = {}
        local i=0
        while i < self:length() do
            start = 8*i
            i = i + 1
            data[i] = redis.call('getrange', self.key, start, start+7)
        end
        return data
    end,
    --
    -- Internal functions
    pack = function(self, value)
        return pack('>d',value)
    end,
    unpack = function(self, value)
        return unpack('>d',value)
    end
}


local columnts_meta = {
    __index = function(self,index)
        return self:get(index)
    end,
    __newindex = function(self,index)
        return self:set(index,value)
    end
}
-- Constructor
function array:new(key)
    local result = {}
    for k,v in pairs(array) do
        result[k] = v
    end
    result:init(key)
    return setmetatable(result, columnts_meta)
end
