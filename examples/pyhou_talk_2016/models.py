import logging, os, sys
import peewee as pw

db_fn = 'example.db'
if os.path.exists(db_fn): os.remove(db_fn)
db = pw.SqliteDatabase(db_fn)

class Person(pw.Model):
  first_name = pw.CharField()
  class Meta:
    database = db

class Pet(pw.Model):
  name = pw.CharField()
  owner = pw.ForeignKeyField(db_column='owner_id', rel_model=Person, to_field='id')
  class Meta:
    database = db

class Blog(pw.Model):
  title = pw.CharField()
  class Meta:
    database = db

class Article(pw.Model):
  title = pw.CharField()
  blog = pw.ForeignKeyField(db_column='blog_id', rel_model=Blog, to_field='id')
  author = pw.ForeignKeyField(db_column='author_id', rel_model=Person, to_field='id')
  editor = pw.ForeignKeyField(db_column='editor_id', rel_model=Person, to_field='id', related_name='edited_articles', null=True)
  reads = pw.IntegerField(default=0)
  class Meta:
    database = db

class Reply(pw.Model):
  text = pw.CharField()
  article = pw.ForeignKeyField(db_column='article_id', rel_model=Article, to_field='id', related_name='replies')
  author = pw.ForeignKeyField(db_column='author_id', rel_model=Person, to_field='id')
  class Meta:
    database = db

class Topic(pw.Model):
  name = pw.CharField()
  class Meta:
    database = db

class ArticleTopic(pw.Model):
  article = pw.ForeignKeyField(rel_model=Article)
  topic = pw.ForeignKeyField(rel_model=Topic)
  class Meta:
    database = db


db.connect()
db.create_tables([Person, Article, Reply, Pet, Blog, Topic, ArticleTopic])

derek = Person.create(first_name='Derek')
callie = Person.create(first_name='Callie')
lingyan = Person.create(first_name='Lingyan')
blog = Blog.create(title='My Blog')
article = Article.create(title='My Article', author=derek, editor=callie, blog=blog)
article = Article.create(title='My Second Article', author=derek, editor=callie, blog=blog)
reply = Reply.create(text='An interesting comment.', article=article, author=lingyan)
chance = Pet.create(name='Chance', owner=derek)
turtle = Pet.create(name='Turtle', owner=callie)
bananas = Topic.create(name='Bananas!!!')
ArticleTopic.create(article=article, topic=bananas)

logger = logging.getLogger('peewee')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


