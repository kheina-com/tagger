from kh_common.exceptions.http_error import BadRequest, InternalServerError
from kh_common.logging import getLogger
from kh_common.sql import SqlInterface
from typing import List
from PIL import Image


class Tagger(SqlInterface) :

	def __init__(self) :
		SqlInterface.__init__(self)
		self.logger = getLogger('tagger')


	def validatePostId(self, post_id: str) :
		if len(post_id) != 8 :
			raise BadRequest('the given post id is invalid.', logdata={ 'post_id': post_id })


	def addTags(self, post_id: str, user_id: int, tags: List[str]) :
		self.validatePostId(post_id)

		try :
			self.query("""
				CALL kheina.public.add_tags(%s, %s, %s);
				""",
				(post_id, user_id, tags,),
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
				(post_id, user_id, tags,),
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
				SELECT tag, class
				FROM kheina.public.tag_post
					INNER JOIN tags
						ON tags.tag_id = tag_post.tag_id
					INNER JOIN tag_classes
						ON tag_classes.class_id = tags.class_id
				WHERE post_id = %s
				UNION SELECT handle, relation
				FROM kheina.public.user_post
					INNER JOIN relations
						ON relations.relation_id = user_post.relation_id
					INNER JOIN users
						ON users.user_id = user_post.user_id
				WHERE post_id = %s;
				""",
				(post_id, user_id, tags,),
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
				'tags': {
					post_id: dict(d)
				}
			}

			refid = uuid4().hex
			self.logger.exception({ 'refid': refid })

		raise BadRequest('no tags were found for the provided post.', logdata={ 'post_id': post_id })
