-- MANAGE ALL COLUMNTS SCRIPTS called by stdnet
local scripts = {
	--
	size = function(self, series, dte)
		local serie = self.on_serie_only(series, 'size')
		return serie:length()
	end,
	-- information regarding the timeserie
	info = function (self, series, start, stop, ...)
	   local serie = self.on_serie_only(series, 'info')
	   local info = serie:info(start, stop, arg)
	   if info then
           return cjson.encode(info)
       end
	end,
	-- Check if *timestamp* exists in the timeseries
	exists = function (self, series, timestamp)
		local serie = self.on_serie_only(series, 'size')
		return serie:exists(timestamp)
	end,
	-- Rank of *timestamp* in timeseries
	rank = function (self, series, timestamp)
		local serie = self.on_serie_only(series, 'size')
		return serie:rank(timestamp)
	end,
	--
	get = function(self, series, dte)
		local serie = self.on_serie_only(series, 'get')
		return self.flatten(serie:get(dte, true))
	end,
	--
	pop = function(self, series, dte)
		local serie = self.on_serie_only(series, 'pop')
		return self.flatten(serie:pop(dte, true))
	end,
	--
	ipop = function(self, series, index)
		local serie = self.on_serie_only(series, 'ipop')
		return self.flatten(serie:ipop(index, true))
	end,
	--
	times = function(self, series, start, stop)
		local serie = self.on_serie_only(series, 'times')
		return serie:times(start, stop)
	end,
	--
	itimes = function(self, series, start, stop)
		local serie = self.on_serie_only(series, 'itimes')
		return serie:itimes(start, stop)
	end,
	--
	range = function(self, series, start, stop, ...)
		local serie = self.on_serie_only(series, 'range')
		return self.flatten(serie:range(start, stop, arg, true))
	end,
	--
	irange = function(self, series, start, stop, ...)
		local serie = self.on_serie_only(series, 'irange', true)
		return self.flatten(serie:irange(start, stop, arg, true))
	end,
	--
	irange_and_delete = function(self, series)
		local serie = self.on_serie_only(series, 'irange_and_delete')
		local result = self.flatten(serie:irange(0, -1, nil, true))
		serie:del()
		return result
	end,
	--
	pop_range = function(self, series, start, stop)
		local serie = self.on_serie_only(series, 'pop_range')
		return self.flatten(serie:pop_range(start, stop, true))
	end,
	--
	ipop_range = function(self, series, start, stop)
		local serie = self.on_serie_only(series, 'ipop_range')
		return self.flatten(serie:ipop_range(start, stop, true))
	end,
	--
	stats = function(self, series, start, stop, ...)
		local fields = {arg}
		local sdata = self.stats_data(series, start, stop, fields, false)
		local serie = self.on_serie_only(sdata, 'stats')
		local result = statistics.univariate(serie)
		if result then
        	return cjson.encode(result)
        end
	end,
	--
	istats = function(self, series, start, stop, ...)
		local fields = {arg}
		local sdata = self.stats_data(series, start, stop, fields, true)
		local serie = self.on_serie_only(sdata, 'istats')
		local result = statistics.univariate(serie)
		if result then
        	return cjson.encode(result)
        end
	end,
	--
	multi_stats = function(self, series, start, stop, fields)
		fields = cjson.decode(fields)
		local sdata = self.stats_data(series, start, stop, fields, false)
		local result = statistics.multivariate(sdata)
		if result then
        	return cjson.encode(result)
        end
	end,
	--
	imulti_stats = function(self, series, start, stop, fields)
		fields = cjson.decode(fields)
		local sdata = self.stats_data(series, start, stop, fields, true)
		local result = statistics.multivariate(sdata)
		if result then
        	return cjson.encode(result)
        end
	end,
	--
	merge = function(self, series, data)
		local serie = self.on_serie_only(series, 'merge')
		serie:del()
		data = cjson.decode(data)
		local fields = data.fields
		local elements = data.series
    	-- First we copy the first timeseries across to self
    	for i, elem in ipairs(elements) do
        	assert( # elem.series <= 2, 'Too many timeseries. Cannot perform operation')
        	-- More than one timeseries. Create the timeseries obtain by multiplying them
        	local ts,tsmul = columnts:new(elem.series[1])
        	if # elem.series == 2 then
            	tsmul = ts
            	ts = columnts:new(elem.series[2])
        	end
        	serie:addserie(ts, elem.weight, fields, tsmul)
    	end
    	return serie:length()
	end,
	-- data is a JSON string containing a table containing the following entries
	-- delete_times, list of times to delete
	-- delete_fields, list of fields to delete
	-- add, list of times,fields to add
	session = function(self, series, data)
		local serie = self.on_serie_only(series, 'session')
		local data = cjson.decode(data)
		for i, t in ipairs(data.delete_times) do
		    serie:pop(t)
		end
		for i, field in ipairs(data.delete_fields) do
            serie:popfield(field)
        end
        for i, time_fields in ipairs(data.add) do
            serie:add(time_fields.times, time_fields.fields)
        end
		return serie:length()
	end,
	--
	-- Run a user script agains the timeserie
	evaluate = function(self, series, script, ...)
		local context = {
			self = series[1],
			series = series,
			others = tabletools.slice(series, 2, -1)
		}
		local script_function = tabletools.load_code(script, context)
		return script_function()
	end,
	--
	----------------------------------------------------------------------------
	-- INTERNALS
	--
	on_serie_only = function(series, name)
		if # series > 1 then
			error(name .. ' requires only one time series.')
		end
		return series[1]
	end,
	--
	flatten = function(time_values)
	    if time_values then
	       return cjson.encode(time_values)
	    end
	end,
	--
	stats_data = function(series, start, stop, fields, byrank)
		local sdata = {}
		local time_values
		for i, serie in ipairs(series) do
        	local fields = fields[i]
        	if byrank then
        		time_values = serie:irange(start, stop, fields)
        	else
        		time_values = serie:range(start, stop, fields)
        	end
        	local t,v = unpack(time_values)
        	table.insert(sdata, {key=serie.key, times=t, field_values=v})
    	end
    	return sdata
	end
}

-- THE FIRST ARGUMENT IS THE NAME OF THE SCRIPT
if # ARGV == 0 then
	error('The first argument must be the name of the script. Got nothing.')
end
local script = scripts[ARGV[1]]
if not script then
	error('Script ' .. ARGV[1] .. ' not available')
end
-- KEYS CONTAIN TIMESERIES IDS
local series = {}
for i, id in ipairs(KEYS) do
	table.insert(series, columnts:new(id))
end
if # series == 0 then
    error('No timeseries available')
end
-- RUN THE SCRIPT
return script(scripts, series, unpack(tabletools.slice(ARGV, 2, -1)))
