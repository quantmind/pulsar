local types = {}
for _, key in ipairs(KEYS) do
    table.insert(types, redis.call('type', key))
end
return types