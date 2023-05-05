from typing import List

from fuzzly.models.post import PostId
from fuzzly.models.tag import Tag, TagGroups
from kh_common.auth import Scope
from kh_common.exceptions.http_error import Forbidden
from kh_common.server import Request, ServerApp

from models import InheritRequest, LookupRequest, RemoveInheritance, TagsRequest, UpdateRequest
from tagger import Tagger


app = ServerApp(
	auth_required = False,
	allowed_hosts = [
		'localhost',
		'127.0.0.1',
		'*.kheina.com',
		'kheina.com',
		'*.fuzz.ly',
		'fuzz.ly',
	],
	allowed_origins = [
		'localhost',
		'127.0.0.1',
		'dev.kheina.com',
		'kheina.com',
		'dev.fuzz.ly',
		'fuzz.ly',
	],
	allowed_methods = [
		'GET',
		'POST',
		'PATCH',
	],
)
tagger = Tagger()


@app.on_event('shutdown')
async def shutdown() :
	tagger.close()

################################################## INTERNAL ##################################################
@app.get('/i1/tags/{post_id}', response_model=TagGroups)
async def i1tags(req: Request, post_id: PostId) -> TagGroups :
	await req.user.verify_scope(Scope.internal)
	return await tagger._fetch_tags_by_post(PostId(post_id))


##################################################  PUBLIC  ##################################################
@app.post('/v1/add_tags', status_code=204)
async def v1AddTags(req: Request, body: TagsRequest) :
	await req.user.authenticated()
	await tagger.addTags(
		req.user,
		body.post_id,
		tuple(body.tags),
	)


@app.post('/v1/remove_tags', status_code=204)
async def v1RemoveTags(req: Request, body: TagsRequest) :
	await req.user.authenticated()
	await tagger.removeTags(
		req.user,
		body.post_id,
		tuple(body.tags),
	)


@app.post('/v1/inherit_tag', status_code=204)
async def v1InheritTag(req: Request, body: InheritRequest) :
	await tagger.inheritTag(
		req.user,
		body.parent_tag,
		body.child_tag,
		body.deprecate,
	)


@app.post('/v1/remove_inheritance', status_code=204)
async def v1RemoveInheritance(req: Request, body: RemoveInheritance) :
	await tagger.removeInheritance(
		req.user,
		body.parent_tag,
		body.child_tag,
	)


@app.patch('/v1/tag/{tag}', status_code=204)
async def v1UpdateTag(req: Request, tag: str, body: UpdateRequest) :
	await req.user.authenticated()

	if Scope.mod not in req.user.scope and body.deprecated is not None :
		raise Forbidden('only mods can edit the deprecated status of a tag.')

	await tagger.updateTag(
		req.user,
		tag,
		body.name,
		body.group,
		body.owner,
		body.description,
		body.deprecated,
	)


@app.get('/v1/fetch_tags/{post_id}', response_model=TagGroups)
@app.get('/v1/tags/{post_id}', response_model=TagGroups)
async def v1FetchTags(req: Request, post_id: PostId) :
	# fastapi does not ensure that postids are in the correct form, so do it manually
	return await tagger.fetchTagsByPost(req.user, PostId(post_id))


@app.post('/v1/lookup_tags', response_model=List[Tag])
async def v1LookUpTags(req: Request, body: LookupRequest) :
	return await tagger.tagLookup(req.user, body.tag)


@app.get('/v1/tag/{tag}', response_model=Tag)
async def v1FetchTag(req: Request, tag: str) :
	return await tagger.fetchTag(req.user, tag)


@app.get('/v1/get_user_tags/{handle}', response_model=List[Tag])
async def v1FetchUserTags(req: Request, handle: str) :
	return await tagger.fetchTagsByUser(req.user, handle)


@app.get('/v1/frequently_used', response_model=TagGroups)
async def v1FrequentlyUsed(req: Request) :
	await req.user.authenticated()
	return await tagger.frequentlyUsed(req.user)


if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='0.0.0.0', port=5002)
