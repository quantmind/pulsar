-- THE FIRST ARGUMENT IS THE COMMAND NAME
if # ARGV == 0 then
	error('The first argument must be the name of the script. Got nothing.')
end
return timeseries.call_mult(ARGV[1], KEYS, unpack(tabletools.slice(ARGV, 2, -1)))
