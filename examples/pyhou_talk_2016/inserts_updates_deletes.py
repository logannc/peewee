import peewee as pw
from models import *


# create the instance
stefan = Person.create(first_name='Stefan')


# update the instance
stefan.first_name = "Steven" # much better
stefan.save()


# bulk update
Person.update(first_name='Derrick').where(Person.first_name=='Derek').execute()


# delete the instance
stefan.delete_instance()


# bulk delete
Person.delete().where(Person.first_name=='Derek').execute()


# burninate the countryside
Person.delete().execute()


# bump read count
Article.update(reads=Article.reads+1).where(Article.id==1).execute()


# bulk insert
Topic.insert_many([
  {'name': 'apple'},
  {'name': 'nyse'},
  {'name': 'tesla'},
]).execute()


