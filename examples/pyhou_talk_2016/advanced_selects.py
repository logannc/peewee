import peewee as pw
from models import *


# inner queries
for p in Person.ALL.where(Person.id.in_(
    Article.select(Article.editor)
  )):
  print p, p.first_name
  

# random sampling
print [p.first_name for p in Person.ALL.order_by(pw.fn.Random()).limit(2)]


# counts
print Person.ALL.count()
print len(Person.ALL.where(Person.first_name != 'Derek'))


# child counts
q = Person.ALL \
    .select(Person, pw.fn.Count(Article.id).alias('article_count')) \
    .join(Article, pw.JOIN.LEFT_OUTER, on=Article.author) \
    .group_by(Article.id)
for p in q:
  print p.first_name, p.article_count
  

# child counts / having
q = Person.ALL \
    .join(Article, pw.JOIN.LEFT_OUTER, on=Article.author) \
    .group_by(Article.id) \
    .having(pw.fn.Count(Article.id)==0)
for p in q:
  print p, p.first_name
  


