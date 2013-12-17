from pulsar.utils.lua import Lua


class Handler():

    def call(self, *args):
        '''A simple function to call from lua'''
        return args


lua = Lua()
print('registering')
lua.register('pytest', Handler(), 'call')
print('testing')
print(lua.execute('return type(pytest)'))
print(lua.execute('return type(pytest.call)'))
result = lua.execute('return pytest.call("ciao")')
print(result)
