from typing import List

from kh_common.auth import Scope
from kh_common.exceptions.http_error import Forbidden
from kh_common.server import NoContentResponse, Request, ServerApp

from models import InheritRequest, LookupRequest, RemoveInheritance, Tag, TagGroups, TagPortable, TagsRequest, UpdateRequest
from tagger import Tagger


app = ServerApp(auth_required=False)
tagger = Tagger()


@app.on_event('shutdown')
async def shutdown() :
	tagger.close()


@app.post('/v1/add_tags', responses={ 204: { 'model': None } }, status_code=204)
async def v1AddTags(req: Request, body: TagsRequest) :
	await req.user.authenticated()
	await tagger.addTags(
		req.user,
		body.post_id,
		tuple(body.tags),
	)
	return NoContentResponse


@app.post('/v1/remove_tags', responses={ 204: { 'model': None } }, status_code=204)
async def v1RemoveTags(req: Request, body: TagsRequest) :
	await req.user.authenticated()
	await tagger.removeTags(
		req.user,
		body.post_id,
		tuple(body.tags),
	)
	return NoContentResponse


@app.post('/v1/inherit_tag', responses={ 204: { 'model': None } }, status_code=204)
async def v1InheritTag(req: Request, body: InheritRequest) :
	await tagger.inheritTag(
		req.user,
		body.parent_tag,
		body.child_tag,
		body.deprecate,
	)
	return NoContentResponse


@app.post('/v1/remove_inheritance', responses={ 204: { 'model': None } }, status_code=204)
async def v1RemoveInheritance(req: Request, body: RemoveInheritance) :
	await tagger.removeInheritance(
		req.user,
		body.parent_tag,
		body.child_tag,
	)
	return NoContentResponse


@app.post('/v1/update_tag', responses={ 204: { 'model': None } }, status_code=204)
async def v1UpdateTag(req: Request, body: UpdateRequest) :
	await req.user.authenticated()

	if Scope.mod not in req.user.scope and body.deprecated is not None :
		raise Forbidden('only mods can edit the deprecated status of a tag.')

	tagger.updateTag(
		req.user,
		body.tag,
		body.name,
		body.tag_class,
		body.owner,
		body.description,
		body.deprecated,
	)
	return NoContentResponse


@app.get('/v1/fetch_tags/{post_id}', responses={ 200: { 'model': TagGroups } })
async def v1FetchTags(req: Request, post_id: str) :
	return await tagger.fetchTagsByPost(req.user, post_id)


@app.post('/v1/lookup_tags')
async def v1LookUpTags(req: Request, body: LookupRequest) :
	return await tagger.tagLookup(req.user, body.tag)


@app.get('/v1/tag/{tag}', responses={ 200: { 'model': Tag } })
async def v1FetchTag(req: Request, tag: str) :
	return await tagger.fetchTag(req.user, tag)


@app.get('/v1/get_user_tags/{handle}', responses={ 200: { 'model': List[Tag] } })
async def v1FetchUserTags(req: Request, handle: str) :
	return await tagger.fetchTagsByUser(req.user, handle)


@app.get('/v1/frequently_used', responses={ 200: { 'model': List[TagPortable] } })
async def v1FrequentlyUsed(req: Request) :
	await req.user.authenticated()
	return await tagger.frequentlyUsed(req.user)


if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='0.0.0.0', port=5002)
