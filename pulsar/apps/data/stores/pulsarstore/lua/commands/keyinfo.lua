-- Retrieve information about keys
-- A list of keys, sorted in alphabetical order, is returned
local start, stop, keys, num
if # ARGV > 0 then -- If argv is provided, it is the pattern to search
    keys = redis.call('KEYS', ARGV[1])
    if # ARGV > 1 then
    	start = ARGV[2] + 0
    	num = ARGV[3] + 0
    else
    	start = 1
    	num = # keys
    end
elseif # KEYS > 0 then -- If keys are provided, use them
    keys = KEYS
    start = 1
    num = # keys
else -- Nothing to do
    return {}
end
local type_table = {}
type_table['set'] = 'scard'
type_table['zset'] = 'zcard'
type_table['list'] = 'llen'
type_table['hash'] = 'hlen'
type_table['ts'] = 'tslen'  -- stdnet branch
type_table['string'] = 'strlen'
local typ, command, len, key, idletime
local stats = {}
local num_keys = # keys
local j = 0
while j < num and start+j <= num_keys do
    key = keys[start+j]
    j = j + 1
    idletime = redis.call('object','idletime',key)
    typ = redis.call('type',key)['ok']
    command = type_table[typ]
    len = 0
    if command then
        len = len + redis.call(command, key)
    end
    stats[j] = {key,typ,len,redis.call('ttl',key),
                redis.call('object','encoding',key),
                idletime}
end
return stats