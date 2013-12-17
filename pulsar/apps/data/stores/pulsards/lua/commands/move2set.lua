-- Move a list of keys to sets or zsets
local N = # KEYS
local moved = 0
local s = ARGV[1]
if N > 0 then
	local typ = 'set'
	if s == 'z' then
		typ = 'zset'
	end
	local n = 0
	while n < N do
		n = n + 1
		local key = KEYS[n]
		if redis_type(key) ~= typ then
			local vals = redis_members(key)
			local timeout = redis.call('ttl', key) + 0
			redis.call('del',key)
			moved = moved + 1
			if typ == 'set' then
				for i,m in pairs(vals) do
					redis.call('sadd', key, m)
				end
			else
				for i,m in pairs(vals) do
					redis.call('zadd', key, 0, m)
				end
			end
			if timeout > 0 then
				redis.call('expire',key,timeout)
			end
		end
	end
end

return {N, moved}