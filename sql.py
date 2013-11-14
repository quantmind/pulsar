
from pulsar.apps.data import odm
from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy.orm import Query


metadata = MetaData()

user = Table('user', metadata,
    Column('user_id', Integer, primary_key=True),
    Column('user_name', String(16), nullable=False, unique=True),
    Column('email_address', String(60)),
    Column('group', String(60), index=True),
    Column('password', String(20), nullable=False)
)

user_data = Table('user_data', metadata,
    Column('user_id', ForeignKey('user.user_id'))
)

router = odm.Router('postgresql://postgres:ciaoluca@127.0.0.1:5432/test',
                    force_sync=True)
loop = router._loop

router.register(user_data)

users = router.user

users._model

#result = users.create_table()

#loop.run_until_complete(result)


result = users.insert(user_name='lsbardel', password='test')

loop.run_until_complete(result)
model
