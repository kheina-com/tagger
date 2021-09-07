from kh_common.exceptions.http_error import BadRequest, Conflict, Forbidden, NotFound, HttpErrorHandler
from models import Tag, TagGroupPortable, TagGroups, TagPortable
from kh_common.caching import ArgsCache, SimpleCache
from kh_common.config.constants import users_host
from kh_common.models.user import UserPortable
from typing import Dict, List, Optional, Tuple
from kh_common.models.privacy import Privacy
from psycopg2.errors import NotNullViolation
from psycopg2.errors import UniqueViolation
from kh_common.models.auth import KhUser
from kh_common.utilities import flatten
from kh_common.sql import SqlInterface
from kh_common.hashing import Hashable
from kh_common.gateway import Gateway
from collections import defaultdict
from kh_common.auth import Scope
from copy import deepcopy
from posts import Posts


postService = Posts()
UsersService = Gateway(users_host + '/v1/fetch_user/{handle}', UserPortable)


class Tagger(SqlInterface, Hashable) :

	def __init__(self) :
		Hashable.__init__(self)
		SqlInterface.__init__(self)


	def _validatePostId(self, post_id: str) :
		if len(post_id) != 8 :
			raise BadRequest('the given post id is invalid.', post_id=post_id)


	def _validateAdmin(self, admin: bool) :
		if not admin :
			raise Forbidden('You must be the tag owner or a mod to edit a tag.')


	def _validateDescription(self, description: str) :
		if len(description) > 1000 :
			raise BadRequest('the given description is invalid, description cannot be over 1,000 characters in length.', description=description)


	@SimpleCache(600)
	def _get_privacy_map(self) :
		data = self.query("""
			SELECT privacy_id, type
			FROM kheina.public.privacy;
			""",
			fetch_all=True,
		)
		return { x[0]: Privacy[x[1]] for x in data if x[1] in Privacy.__members__ }


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
			(user_id, parent_tag, child_tag.lower(), deprecate),
			commit=True,
		)


	@ArgsCache(60)
	@HttpErrorHandler('updating a tag')
	def updateTag(self, user: KhUser, tag: str, name: str, tag_class: str, owner: str, description: str) :
		query = []
		params = []

		if not any([name, tag_class, owner, description]) :
			raise BadRequest('no params were provided.')

		with self.transaction() as transaction :
			data = transaction.query(
				"""
				SELECT tags.owner
				FROM kheina.public.tags
				WHERE tags.tag = %s
				""",
				(tag,),
				fetch_one=True,
			)

			current_owner = data[0] if data else None

			if user.user_id != current_owner and Scope.mod not in user.scope :
				raise Forbidden('You must be the tag owner or a mod to edit a tag.')

			if tag_class :
				query.append('class_id = tag_class_to_id(%s)')
				params.append(tag_class)

			if name :
				query.append('tag = %s')
				params.append(name.lower())

			if owner :
				query.append('owner = user_to_id(%s)')
				params.append(owner)

			if description :
				self._validateDescription(description)
				query.append('description = %s')
				params.append(description)

			try :
				transaction.query(f"""
					UPDATE kheina.public.tags
					SET {','.join(query)}
					WHERE tags.tag = %s
					""",
					params + [tag],
				)

			except NotNullViolation :
				raise BadRequest('The tag class you entered could not be found or does not exist.')

			except UniqueViolation :
				raise Conflict('A tag with that name already exists.')

			transaction.commit()


	@ArgsCache(60)
	@HttpErrorHandler('fetching user-owned tags')
	async def fetchTagsByUser(self, user: KhUser, handle: str) :
		data = [
			Tag(
				**load,
				owner = await UsersService(
					handle=load['handle'],
					auth=user.token.token_string,
				),
			)
			for _, load in self._pullAllTags().items() if load['handle'] == handle
		]

		if not data :
			raise NotFound('the provided user does not exist or the user does not own any tags.', handle=handle)

		return data


	@ArgsCache(5)
	def _fetchTagsByPost(self, post_id: str) :
		data = self.query("""
			SELECT tag_classes.class, array_agg(tags.tag), posts.privacy_id, posts.uploader
			FROM kheina.public.posts
				LEFT JOIN kheina.public.tag_post
					ON tag_post.post_id = posts.post_id
				LEFT JOIN kheina.public.tags
					ON tags.tag_id = tag_post.tag_id
						AND tags.deprecated = false
				LEFT JOIN kheina.public.tag_classes
					ON tag_classes.class_id = tags.class_id
			WHERE posts.post_id = %s
			GROUP BY tag_classes.class_id, posts.privacy_id, posts.uploader;
			""",
			(post_id,),
			fetch_all=True,
		)

		if not data :
			raise NotFound("the provided post does not exist or you don't have access to it.", post_id=post_id)

		return {
			'tags': TagGroups({
				TagGroupPortable(i[0]): sorted(map(TagPortable, filter(None, i[1])))
				for i in data
				if i[0]
			}),
			'privacy': self._get_privacy_map()[data[0][2]],
			'user_id': data[0][3],
		}


	@HttpErrorHandler('fetching tags by post')
	async def fetchTagsByPost(self, user: KhUser, post_id: str) -> TagGroups :
		self._validatePostId(post_id)

		data = self._fetchTagsByPost(post_id)

		if (
			data['privacy'] not in { Privacy.public, Privacy.unlisted }
			and (
				data['user_id'] != user.user_id
				or not await user.authenticated(raise_error=False)
			)
		) :
			# the post was found and returned, but the user shouldn't have access to it or isn't authenticated
			raise NotFound("the provided post does not exist or you don't have access to it.", post_id=post_id)

		return data['tags']


	@SimpleCache(60)
	def _pullAllTags(self) :
		data = self.query("""
			SELECT
				tag_classes.class,
				tags.tag,
				tags.deprecated,
				array_agg(t2.tag),
				users.handle,
				tags.description
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
				'tag': row[1],
				'group': TagGroupPortable(row[0]),
				'deprecated': row[2],
				'inherited_tags': list(map(TagPortable, filter(None, row[3]))),
				'handle': row[4],
				'description': row[5],
			}
			for row in data
		}


	@HttpErrorHandler('looking up tags')
	def tagLookup(self, tag: Optional[str] = None) :
		t = tag or ''

		data = self._pullAllTags()
		tags = { }

		for tag, load in deepcopy(data).items() :

			if not tag.startswith(t) :
				continue

			tag_class = load.group

			if tag_class in tags :
				tags[tag_class][tag] = load

			else :
				tags[tag_class] = { tag: load }

		return tags


	@HttpErrorHandler('fetching tag')
	async def fetchTag(self, user: KhUser, tag: str) :
		data = self._pullAllTags()

		if tag not in data :
			raise NotFound('the provided tag does not exist.', tag=tag)

		return Tag(
			**data[tag],
			owner = await UsersService(
				handle=data[tag]['handle'],
				auth=user.token.token_string,
			),
		)


	@ArgsCache(60)
	@HttpErrorHandler('fetching frequently used tags')
	async def frequentlyUsed(self, user: KhUser) -> List[TagPortable] :
		posts = await postService.userPosts(user)

		tags = defaultdict(lambda : 0)

		for post in posts :
			postTags = await self.fetchTagsByPost(user, post.post_id)

			for tag in flatten(postTags) :
				tags[tag] += 1

		return list(map(lambda x : TagPortable(x[0]), sorted(tags.items(), key=lambda x : x[1], reverse=True)))[:25]
