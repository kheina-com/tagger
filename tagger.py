from asyncio import Task, ensure_future, wait
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Union

from kh_common.auth import KhUser, Scope
from kh_common.caching import ArgsCache, SimpleCache
from kh_common.caching.key_value_store import KeyValueStore
from kh_common.config.constants import posts_host, users_host
from kh_common.exceptions.http_error import BadRequest, Conflict, Forbidden, HttpErrorHandler, NotFound
from kh_common.gateway import Gateway
from kh_common.hashing import Hashable
from kh_common.models.privacy import Privacy
from kh_common.models.user import UserPortable
from kh_common.sql import SqlInterface
from psycopg2.errors import NotNullViolation, UniqueViolation

from models import Post, Tag, TagGroupPortable, TagGroups, TagPortable


UsersService = Gateway(users_host + '/v1/fetch_user/{handle}', UserPortable)
PostsService = Gateway(posts_host + '/v1/fetch_my_posts', List[Post], method='POST')
PostsBody = { 'sort': 'new', 'count': 64, 'page': 1 }
Misc: TagGroupPortable = TagGroupPortable('misc')
CountKVS: KeyValueStore = KeyValueStore('kheina', 'tag_count')


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


	def _populate_tag_cache(self, tag: str) -> None :
		if not CountKVS.exists(tag) :
			# we gotta populate it here (sad)
			data = self.query("""
				SELECT count(tag_post.post_id)
				FROM kheina.public.tags
					INNER JOIN kheina.public.tag_post
						ON tags.tag_id = tag_post.tag_id
				WHERE tags.tag = %s
					AND tags.deprecated = false;
				""",
				(tag,),
				fetch_one=True,
			)
			CountKVS.put(tag, int(data[0]), -1)


	def _get_tag_count(self, tag: str) -> int :
		self._populate_tag_cache(tag)
		return CountKVS.get(tag)


	def _increment_tag_count(self, tag: str) -> None :
		self._populate_tag_cache(tag)
		KeyValueStore._client.increment(
			(CountKVS._namespace, CountKVS._set, tag),
			'data',
			1,
			meta={
				'ttl': -1,
			},
			policy={
				'max_retries': 3,
			},
		)


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

		all_tags = self._pullAllTags()
		for tag in tags :
			all_tags[tag].increment(1)


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
	async def inheritTag(self, user: KhUser, parent_tag: str, child_tag: str, deprecate:bool=False) :
		await user.verify_scope(Scope.admin)

		await self.query_async("""
			CALL kheina.public.inherit_tag(%s, %s, %s, %s);
			""",
			(user.user_id, parent_tag.lower(), child_tag.lower(), deprecate),
			commit=True,
		)


	@ArgsCache(60)
	@HttpErrorHandler('removing tag inheritance')
	async def removeInheritance(self, user: KhUser, parent_tag: str, child_tag: str) :
		await user.verify_scope(Scope.admin)

		await self.query_async("""
			DELETE FROM kheina.public.tag_inheritance
				USING kheina.public.tags as t1,
					kheina.public.tags as t2
			WHERE tag_inheritance.parent = t1.tag_id
				AND t1.tag = %s
				AND tag_inheritance.child = t2.tag_id
				AND t2.tag = %s;
			""",
			(parent_tag.lower(), child_tag.lower()),
			commit=True,
		)


	@ArgsCache(60)
	@HttpErrorHandler('updating a tag')
	def updateTag(self, user: KhUser, tag: str, name: str, tag_class: str, owner: str, description: str, deprecated: bool = None) :
		query = []
		params = []

		if not any([name, tag_class, owner, description, deprecated is not None]) :
			raise BadRequest('no params were provided.')

		with self.transaction() as transaction :
			data = transaction.query(
				"""
				SELECT tags.owner
				FROM kheina.public.tags
				WHERE tags.tag = %s
				""",
				(tag.lower(),),
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

			if deprecated is not None :
				query.append('deprecated = %s')
				params.append(deprecated)

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
				owner=await UsersService(
					handle=load['handle'],
					auth=user.token.token_string if user.token else None,
				),
				count=self._get_tag_count(tag),
			)
			for tag, load in self._pullAllTags().items() if load['handle'] == handle
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
				'tag': TagPortable(row[1]),
				'group': TagGroupPortable(row[0]),
				'deprecated': row[2],
				'inherited_tags': list(map(TagPortable, filter(None, row[3]))),
				'handle': row[4],
				'description': row[5],
			}
			for row in data
		}


	async def _populate_tag_misc(self, user: KhUser, tag: Dict[str, Union[TagPortable, TagGroupPortable, bool, List[TagPortable], str]]) -> Tag :
		if tag['handle'] :
			return Tag(
				**tag,
				owner=await UsersService(
					handle=tag['handle'],
					auth=user.token.token_string if user.token else None,
				),
				count=self._get_tag_count(tag['tag']),
			)

		tag['count'] = self._get_tag_count(tag['tag']),
		return Tag.parse_obj(tag)


	@HttpErrorHandler('looking up tags')
	async def tagLookup(self, user: KhUser, tag: Optional[str] = None) :
		t: str = tag or ''

		tags: List[Tag] = []

		for tag, load in self._pullAllTags().items() :

			if not tag.startswith(t) :
				continue

			tags.append(ensure_future(self._populate_tag_misc(user, load)))

		await wait(tags)

		return list(map(Task.result, tags))


	@HttpErrorHandler('fetching tag')
	async def fetchTag(self, user: KhUser, tag: str) :
		data = self._pullAllTags()

		if tag not in data :
			raise NotFound('the provided tag does not exist.', tag=tag)

		return await self._populate_tag_misc(user, data[tag])


	@ArgsCache(60)
	@HttpErrorHandler('fetching frequently used tags')
	async def frequentlyUsed(self, user: KhUser) -> TagGroups :
		posts: List[Post] = await PostsService(PostsBody, auth=user.token.token_string)

		# set up all the tags to be fetched async
		post_tags: List[Task[TagGroups]] = list(map(lambda post : ensure_future(self.fetchTagsByPost(user, post.post_id)), posts))

		tags = defaultdict(lambda : defaultdict(lambda : 0))

		for tag_set in post_tags :
			for group, tag_list in (await tag_set).items() :
				for tag in tag_list :
					tags[group][tag] += 1

		return TagGroups({
			TagGroupPortable(group): list(map(
				lambda x : TagPortable(x[0]),
				sorted(tag_ranks.items(), key=lambda x : x[1], reverse=True)
			))[:(25 if group == Misc else 10)]
			for group, tag_ranks in tags.items()
		})
