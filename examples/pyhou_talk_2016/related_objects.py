import peewee as pw
from models import *


print '-'*80

# the dumb way
for article in Article.ALL:
  print 'title', article.title
  print 'author', article.author.first_name
  print 'editor', article.editor.first_name

  
print '-'*80

# a bit better
for article in Article.ALL.plus(Article.author):
  print 'title', article.title
  print 'author', article.author.first_name
  print 'editor', article.editor.first_name


print '-'*80

# much better
for article in Article.ALL.plus(Article.author).plus(Article.editor):
  print 'title', article.title
  print 'author', article.author.first_name
  print 'editor', article.editor.first_name


print '-'*80

# who edited all the articles i wrote?
q = Article.ALL \
  .plus(Article.author.as_('author')) \
  .plus(Article.editor) \
  .where(Person.as_('author').first_name=='Derek')
for article in q:
  print 'title', article.title
  print 'author', article.author.first_name
  print 'editor', article.editor.first_name



print '-'*80

# the peewee way
author_table = Person.alias()
editor_table = Person.alias()
q = Reply.select(Reply, author_table, editor_table) \
  .join(Article) \
  .join(author_table, join_type=pw.JOIN.LEFT_OUTER, on=(author_table==Article.author)) \
  .switch(Article) \
  .join(editor_table, join_type=pw.JOIN.LEFT_OUTER, on=(editor_table==Article.editor)) \
  .where(author_table.first_name=="Derek")
print q.sql()


# vs. the plus method
q = Reply.ALL \
  .plus(Reply.article, Article.author.as_('author')) \
  .plus(Reply.article, Article.editor) \
  .where(Person.as_('author').first_name=="Derek")
print q.sql()


print '-'*80

# grab everyone w/ all their articles
for person in Person.ALL.plus(Article.author):
  print person.first_name, person.article_set


print '-'*80

# grab everyone w/ all their articles and the article editor
for person in Person.ALL.plus(Article.author, Article.editor):
  articles = [{'article':article.title, 'editor':article.editor.first_name} for article in person.article_set]
  print person.first_name, articles


print '-'*80

# article with all their topics
for article in Article.ALL.plus(ArticleTopic.article, ArticleTopic.topic):
  print article.title, [at.topic.name for at in article.articletopic_set]



