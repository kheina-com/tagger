from kh_common.exceptions.http_error import BadRequest, Forbidden, NotFound, InternalServerError
from kh_common.caching import ArgsCache, SimpleCache
from typing import Optional, Dict, List, Tuple
from psycopg2.errors import UniqueViolation
from kh_common.logging import getLogger
from kh_common.sql import SqlInterface
from uuid import uuid4


class Tagger(SqlInterface) :

	_hash = 0

	def __init__(self) :
		SqlInterface.__init__(self)
		self.hash = int.from_bytes(f"<class 'Tagger' {Tagger._hash}>".encode(), 'big')
		Tagger._hash += 1


	def __hash__(self) :
		return self.hash


	def _validatePostId(self, post_id: str) :
		if len(post_id) != 8 :
			raise BadRequest('the given post id is invalid.', logdata={ 'post_id': post_id })


	@ArgsCache(60)
	def addTags(self, user_id: int, post_id: str, tags: Tuple[str]) :
		self._validatePostId(post_id)

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


	@ArgsCache(60)
	def removeTags(self, user_id: int, post_id: str, tags: Tuple[str]) :
		self._validatePostId(post_id)

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


	@ArgsCache(60)
	def inheritTag(self, user_id: int, parent_tag: str, child_tag: str, deprecate:bool=False, admin:bool=False) :
		try :
			data = self.query("""
				CALL kheina.public.inherit_tag(%s, %s, %s, %s);
				""",
				(user_id, parent_tag, child_tag, deprecate),
				commit=True,
			)

		except :
			refid = uuid4().hex
			logdata = {
				'refid': refid,
				'parent_tag': parent_tag,
				'child_tag': child_tag,
				'deprecate': deprecate,
				'user_id': user_id,
			}
			self.logger.exception(logdata)
			raise InternalServerError('an error occurred while adding a new tag inheritance.', logdata=logdata)


	@ArgsCache(60)
	def updateTag(self, user_id: int, tag: str, tag_class:str=None, owner:str=None, admin:bool=False) :
		query = []
		params = []

		if tag_class :
			query.append('SET class_id = tag_class_to_id(%s)')
			params.append(tag_class)

		if owner :
			if not admin :
				raise Forbidden('only admins are allowed to use this function.')
			query.append('SET owner = user_to_id(%s)')
			params.append(owner)

		if not params :
			raise BadRequest('no params were provided.')

		try :
			self.query(f"""
				UPDATE kheina.public.tags
				{','.join(query)}
				WHERE tags.tag = %s AND (
					owner IS NULL
					OR owner = %s
				)
				""",
				params + [tag, user_id],
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


	@ArgsCache(60)
	def fetchTagsByPost(self, user_id: int, post_id: str) :
		self._validatePostId(post_id)

		try :
			data = self.query("""
				SELECT tag_classes.class, array_agg(tags.tag)
				FROM kheina.public.tag_post
					INNER JOIN kheina.public.tags
						ON tags.tag_id = tag_post.tag_id
							AND tags.deprecated = false
					INNER JOIN kheina.public.tag_classes
						ON tag_classes.class_id = tags.class_id
				WHERE post_id = %s
				GROUP BY class;
				""",
				(post_id,),
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
				post_id: dict(data),
			}

		raise NotFound("the provided post does not exist or you don't have access to it.", logdata={ 'post_id': post_id })


	@SimpleCache(60)
	def _pullAllTags(self) :
		try :
			data = self.query("""
				SELECT tag_classes.class, tags.tag, tags.deprecated, array_agg(t2.tag), users.handle
				FROM tags
					INNER JOIN tag_classes
						ON tag_classes.class_id = tags.class_id
					LEFT JOIN tag_inheritance
						ON tag_inheritance.parent = tags.tag_id
					LEFT JOIN tags as t2
						ON t2.tag_id = tag_inheritance.child
					LEFT JOIN users
						ON users.user_id = tags.owner
				GROUP BY tags.tag_id, tag_classes.class_id, users.user_id;
				""",
				fetch_all=True,
			)

		except :
			refid = uuid4().hex
			logdata = {
				'refid': refid,
				'tag': tag,
			}
			self.logger.exception(logdata)
			raise InternalServerError('an error occurred while retrieving tags.', logdata=logdata)

		return data


	def tagLookup(self, tag:Optional[str]=None) :
		tag = tag or ''

		data = self._pullAllTags()

		tags = { }
		for i in data :
			if not i[1].startswith(tag) :
				continue

			if i[0] in tags :
				tags[i[0]][i[1]] = { 'deprecated': i[2], 'inherited_tags': list(filter(None, i[3])), 'owner': i[4] }
			else :
				tags[i[0]] = { i[1]: { 'deprecated': i[2], 'inherited_tags': list(filter(None, i[3])), 'owner': i[4] } }

		return tags
