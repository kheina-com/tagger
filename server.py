from models import InheritRequest, LookupRequest, PostRequest, TagsRequest, UpdateRequest
from kh_common.server import Request, ServerApp, UJSONResponse, NoContentResponse
from kh_common.auth import Scope
from tagger import Tagger


app = ServerApp(auth_required=False)
tagger = Tagger()


@app.on_event('shutdown')
async def shutdown() :
	tagger.close()


@app.post('/v1/add_tags')
async def v1AddTags(req: Request, body: TagsRequest) :
	req.user.authenticated()
	tagger.addTags(
		req.user.user_id,
		body.post_id,
		tuple(body.tags),
	)
	return NoContentResponse


@app.post('/v1/remove_tags')
async def v1RemoveTags(req: Request, body: TagsRequest) :
	req.user.authenticated()
	tagger.removeTags(
		req.user.user_id,
		body.post_id,
		tuple(body.tags),
	)
	return NoContentResponse


@app.post('/v1/inherit_tag')
async def v1InheritTag(req: Request, body: InheritRequest) :
	req.user.authenticated()
	tagger.inheritTag(
		req.user.user_id,
		body.parent_tag,
		body.child_tag,
		body.deprecate,
		Scope.admin in req.user.scope,
	)
	return NoContentResponse


@app.post('/v1/update_tag')
async def v1UpdateTag(req: Request, body: UpdateRequest) :
	req.user.authenticated()
	tagger.updateTag(
		req.user.user_id,
		body.tag,
		body.tag_class,
		body.owner,
		Scope.mod in req.user.scope,
	)
	return NoContentResponse


@app.get('/v1/fetch_tags/{post_id}')
async def v1FetchTags(post_id: str) :
	return UJSONResponse(
		tagger.fetchTagsByPost(post_id),
	)


@app.post('/v1/lookup_tags')
async def v1FetchTags(body: LookupRequest) :
	return UJSONResponse(
		tagger.tagLookup(body.tag),
	)


@app.get('/v1/tag/{tag}')
async def v1FetchTag(tag: str) :
	return UJSONResponse(
		tagger.fetchTag(tag),
	)


@app.get('/v1/get_user_tags/{handle}')
async def v1FetchUserTags(handle: str) :
	return UJSONResponse(
		tagger.fetchTagsByUser(handle),
	)


if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='0.0.0.0', port=5002)
