local command = ARGV[1]
local start = ARGV[2]
local stop = ARGV[3]
local points = math.max(ARGV[4] + 0, 2)
local method = ARGV[5]  --  One of 'mean', 'geometric', 'ma'
local alpha = ARGV[6] + 0 -- paramether for moving avaerage reduction
local num_fields = ARGV[7]
local fields = tabletools.slice(ARGV, 8, -1)
local ts = columnts:new(KEYS[1])
local time,values = unpack(ts:range(command, start, stop, fields))
local N = # times
if N < points then
    return {times,values}
else
    local space = math.floor(N/points)  -- spacing for reduction
    while N/space > points do
        space = space + 1
    end 
    local rtime, reduced = {}, {}
    for field,value do
        local index, stop, fvalues = 1, 0, {}
        table.insert(reduced,field)
        table.insert(reduced,fvalues)
        while stop <= N do
            start = stop + 1
            stop = N - (points-index)*space
            rtime[index] = time[stop]
            if method == 'mean' then
                fvalues[index] = reduce_mean(value,start,stop)
            elseif method == 'geometric' then
                fvalues[index] = reduce_geo(value,start,stop)
            elseif method == 'ma' then
                fvalues[index] = reduce_ma(value,start,stop,alpha)
            else
                fvalues[index] = value[stop]
            end
            index = index + 1
        end
    end
    return {rtime, reduced}
end


