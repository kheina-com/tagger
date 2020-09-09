from kh_common.exceptions.http_error import BadRequest, InternalServerError
from kh_common.logging import getLogger
from kh_common.sql import SqlInterface
from typing import List
from uuid import uuid4
from PIL import Image


class Tagger(SqlInterface) :

	def __init__(self) :
		SqlInterface.__init__(self)
		self.logger = getLogger('tagger')


	def validatePostId(self, post_id: str) :
		if len(post_id) != 8 :
			raise BadRequest('the given post id is invalid.', logdata={ 'post_id': post_id })


	def validatePageNumber(self, page_number: int) :
		if page_number < 1 :
			raise BadRequest('the given page number is invalid.', logdata={ 'post_id': post_id })


	def validateCount(self, count: int) :
		if count < 1 :
			raise BadRequest('the given count is invalid.', logdata={ 'post_id': post_id })


	def addTags(self, post_id: str, user_id: int, tags: List[str]) :
		self.validatePostId(post_id)

		try :
			self.query("""
				CALL kheina.public.add_tags(%s, %s, %s);
				""",
				(post_id, user_id, tags),
				commit=True,
			)

		except :
			refid = uuid4().hex
			logdata = {
				'refid': refid,
				'post_id': post_id,
				'user_id': user_id,
				'tags': tags,
			}
			self.logger.exception(logdata)
			raise InternalServerError('an error occurred while adding tags for provided post.', logdata=logdata)


	def removeTags(self, post_id: str, user_id: int, tags: List[str]) :
		self.validatePostId(post_id)

		try :
			self.query("""
				CALL kheina.public.remove_tags(%s, %s, %s);
				""",
				(post_id, user_id, tags),
				commit=True,
			)

		except :
			refid = uuid4().hex
			logdata = {
				'refid': refid,
				'post_id': post_id,
				'user_id': user_id,
				'tags': tags,
			}
			self.logger.exception(logdata)
			raise InternalServerError('an error occurred while removing tags for provided post.', logdata=logdata)


	def fetchTags(self, post_id: str) :
		self.validatePostId(post_id)

		try :
			data = self.query("""
				SELECT class, array_agg(tag)
				FROM kheina.public.tag_post
					INNER JOIN kheina.public.tags
						ON tags.tag_id = tag_post.tag_id
					INNER JOIN kheina.public.tag_classes
						ON tag_classes.class_id = tags.class_id
				WHERE post_id = %s
				GROUP BY class
				UNION SELECT relation, array_agg(handle)
				FROM kheina.public.user_post
					INNER JOIN kheina.public.relations
						ON relations.relation_id = user_post.relation_id
					INNER JOIN kheina.public.users
						ON users.user_id = user_post.user_id
				WHERE post_id = %s
				GROUP BY relation;
				""",
				(post_id, post_id),
				fetch_all=True,
			)

		except :
			refid = uuid4().hex
			logdata = {
				'refid': refid,
				'post_id': post_id,
			}
			self.logger.exception(logdata)
			raise InternalServerError('an error occurred while retrieving tags for provided post.', logdata=logdata)

		if data :
			return {
				post_id: dict(data)
			}

		raise BadRequest('no tags were found for the provided post.', logdata={ 'post_id': post_id })


	def fetchPosts(self, user_id: int, tags: List[str], count:int=64, page:int=1) :
		self.validatePageNumber(page)
		self.validateCount(count)

		try :
			self.query("""
				CALL kheina.public.fetch_posts_by_tag(%s, %s, %s, %s);
				""",
				(tags, user_id, count, page - 1),
				commit=True,
			)

		except :
			refid = uuid4().hex
			logdata = {
				'refid': refid,
				'page': page,
				'user_id': user_id,
				'tags': tags,
			}
			self.logger.exception(logdata)
			raise InternalServerError('an error occurred while fetching posts.', logdata=logdata)

		if data :
			return {
				'posts': data
			}

		raise BadRequest('no posts were found for the provided tags and page.', logdata={ 'tags': tags, 'page': page })
