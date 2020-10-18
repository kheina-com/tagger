from models import InheritRequest, LookupRequest, PostRequest, TagsRequest, UpdateRequest
from starlette.middleware.trustedhost import TrustedHostMiddleware
from kh_common.exceptions import jsonErrorHandler
from starlette.responses import UJSONResponse
from kh_common.auth import KhAuthMiddleware
from fastapi import FastAPI, Request
from tagger import Tagger


app = FastAPI()
app.add_exception_handler(Exception, jsonErrorHandler)
app.add_middleware(TrustedHostMiddleware, allowed_hosts={ 'localhost', '127.0.0.1', 'tags.kheina.com', 'tags-dev.kheina.com' })
app.add_middleware(KhAuthMiddleware)

tagger = Tagger()


@app.on_event('shutdown')
async def shutdown() :
	tagger.close()


@app.post('/v1/add_tags')
async def v1AddTags(req: Request, body: TagsRequest) :
	"""
	{
		"post_id": str,
		"tags": [
			str
		]
	}
	"""

	return UJSONResponse(
		tagger.addTags(
			req.user.user_id,
			body.post_id,
			tuple(body.tags),
		)
	)


@app.post('/v1/remove_tags')
async def v1RemoveTags(req: Request, body: TagsRequest) :
	"""
	{
		"post_id": str,
		"tags": [
			str
		]
	}
	"""

	return UJSONResponse(
		tagger.removeTags(
			req.user.user_id,
			body.post_id,
			tuple(body.tags),
		)
	)


@app.post('/v1/inherit_tag')
async def v1InheritTag(req: Request, body: InheritRequest) :
	"""
	{
		"parent_tag": str,
		"child_tag": str,
		"deprecate": Optional[bool],
		"admin": Optional[bool]
	}
	"""

	return UJSONResponse(
		tagger.inheritTag(
			req.user.user_id,
			body.parent_tag,
			body.child_tag,
			body.deprecate,
			'admin' in req.user.scopes,
		)
	)


@app.post('/v1/update_tag')
async def v1UpdateTag(req: Request, body: UpdateRequest) :
	"""
	{
		"tag": str,
		"tag_class": Optional[str],
		"owner": Optional[str],
		"admin": Optional[bool]
	}
	"""

	return UJSONResponse(
		tagger.updateTag(
			req.user.user_id,
			body.tag,
			body.tag_class,
			body.owner,
			'admin' in req.user.scopes,
		)
	)


@app.post('/v1/fetch_tags')
async def v1FetchTags(req: Request, body: PostRequest) :
	"""
	{
		"post_id": str
	}
	"""

	return UJSONResponse(
		tagger.fetchTagsByPost(
			req.user.user_id,
			body.post_id,
		)
	)


@app.post('/v1/lookup_tags')
async def v1FetchTags(req: Request, body: LookupRequest) :
	"""
	{
		"tag": str
	}
	"""

	return UJSONResponse(
		tagger.tagLookup(body.tag)
	)


if __name__ == '__main__' :
	from uvicorn.main import run
	run(app, host='127.0.0.1', port=5002)
