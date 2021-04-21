from kh_common.exceptions.http_error import BadRequest, Forbidden, NotFound, InternalServerError, HttpErrorHandler
from kh_common.caching import ArgsCache, SimpleCache
from typing import Dict, List, Optional, Tuple
from psycopg2.errors import UniqueViolation
from kh_common.logging import getLogger
from kh_common.sql import SqlInterface
from kh_common.hashing import Hashable
from copy import deepcopy
from uuid import uuid4


class Tagger(SqlInterface, Hashable) :

	def __init__(self) :
		Hashable.__init__(self)
		SqlInterface.__init__(self)


	def _validatePostId(self, post_id: str) :
		if len(post_id) != 8 :
			raise BadRequest('the given post id is invalid.', logdata={ 'post_id': post_id })


	def _validateAdmin(self, admin: bool) :
		if not admin :
			raise Forbidden('only admins are allowed to use this function.')


	@ArgsCache(60)
	@HttpErrorHandler('adding tags to post')
	def addTags(self, user_id: int, post_id: str, tags: Tuple[str]) :
		self._validatePostId(post_id)

		self.query("""
			CALL kheina.public.add_tags(%s, %s, %s);
			""",
			(post_id, user_id, list(map(str.lower, tags))),
			commit=True,
		)


	@ArgsCache(60)
	@HttpErrorHandler('removing tags from post')
	def removeTags(self, user_id: int, post_id: str, tags: Tuple[str]) :
		self._validatePostId(post_id)

		self.query("""
			CALL kheina.public.remove_tags(%s, %s, %s);
			""",
			(post_id, user_id, list(map(str.lower, tags))),
			commit=True,
		)


	@ArgsCache(60)
	@HttpErrorHandler('inheriting a tag')
	def inheritTag(self, user_id: int, parent_tag: str, child_tag: str, deprecate:bool=False, admin:bool=False) :
		self._validateAdmin(admin)

		data = self.query("""
			CALL kheina.public.inherit_tag(%s, %s, %s, %s);
			""",
			(user_id, parent_tag, child_tag, deprecate),
			commit=True,
		)


	@ArgsCache(60)
	@HttpErrorHandler('updating a tag')
	def updateTag(self, user_id: int, tag: str, tag_class:str=None, owner:str=None, admin:bool=False) :
		query = []
		params = []

		if tag_class :
			query.append('SET class_id = tag_class_to_id(%s)')
			params.append(tag_class)

		if owner :
			self._validateAdmin(admin)
			query.append('SET owner = user_to_id(%s)')
			params.append(owner)

		if not params :
			raise BadRequest('no params were provided.')

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


	@ArgsCache(60)
	@HttpErrorHandler('fetching user-owned tags')
	def fetchTagsByUser(self, handle: str) :
		data = self.query("""
			SELECT tags.tag
			FROM kheina.public.users
				INNER JOIN kheina.public.tags
					ON tags.owner = users.user_id
			WHERE lower(users.handle) = %s;
			""",
			(handle.lower(),),
			fetch_all=True,
		)

		if not data :
			raise NotFound('the provided user does not exist or the user does not own any tags.', logdata={ 'handle': handle })

		return [
			i[0] for i in data
		]


	@ArgsCache(60)
	@HttpErrorHandler('fetching tags by post')
	def fetchTagsByPost(self, post_id: str) :
		self._validatePostId(post_id)

		data = self.query("""
			SELECT tag_classes.class, array_agg(tags.tag)
			FROM kheina.public.posts
				LEFT JOIN kheina.public.tag_post
					ON tag_post.post_id = posts.post_id
				LEFT JOIN kheina.public.tags
					ON tags.tag_id = tag_post.tag_id
						AND tags.deprecated = false
				LEFT JOIN kheina.public.tag_classes
					ON tag_classes.class_id = tags.class_id
			WHERE posts.post_id = %s
			GROUP BY tag_classes.class_id;
			""",
			(post_id,),
			fetch_all=True,
		)

		if data :
			return {
				i[0]: sorted(filter(None, i[1]))
				for i in data
				if i[0]
			}

		raise NotFound("the provided post does not exist or you don't have access to it.", logdata={ 'post_id': post_id })


	@SimpleCache(60)
	def _pullAllTags(self) :
		data = self.query("""
			SELECT tag_classes.class, tags.tag, tags.deprecated, array_agg(t2.tag), users.handle, users.display_name, users.icon
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

		return {
			row[1]: {
				'class': row[0],
				'deprecated': row[2],
				'inherited_tags': list(filter(None, row[3])),
				'owner': {
					'handle': row[4],
					'name': row[5],
					'icon': row[6],
				} if row[4] else None,
			}
			for row in data
		}


	@HttpErrorHandler('looking up tags')
	def tagLookup(self, tag:Optional[str]=None) :
		t = tag or ''

		data = self._pullAllTags()
		tags = { }

		for tag, load in deepcopy(data).items() :

			if not t.startswith(t) :
				continue

			tag_class = load.pop('class')

			if tag_class in tags :
				tags[tag_class][tag] = load

			else :
				tags[tag_class] = { tag: load }

		return tags


	@HttpErrorHandler('fetching tag')
	def fetchTag(self, tag: str) :
		data = self._pullAllTags()

		if tag not in data :
			raise NotFound('the provided tag does not exist.', logdata={ 'tag': tag })

		return {
			'tag': tag,
			**data[tag],
		}
