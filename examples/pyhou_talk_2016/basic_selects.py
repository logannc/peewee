import peewee as pw
from models import *


# all the rows
for p in Person.ALL:
  print p, p.first_name


# basic condition
for p in Person.ALL.where(Person.first_name=='Derek'):
  print p, p.first_name


# get's the first object in a query
print Person.ALL.first().first_name


# get asserts only one result
print Person.ALL.get(Person.first_name=='Derek').first_name
try:
  print Person.ALL.get().first_name
except Exception as e:
  print e


# case-sensitive like (does not seem to workin sqlite)
print Person.ALL.where(Person.first_name % 'Der%').first()


# case-insensitive like
# sqlite's LIKE is by default case-insensitive,
# but docs say other dbs will use ILIKE
print Person.ALL.where(Person.first_name ** 'der%').get().first_name


# ordering
print [p.first_name for p in Person.ALL.order_by(Person.first_name)]



