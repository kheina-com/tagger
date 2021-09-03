from aiohttp import ClientTimeout, request as async_request
from kh_common.config.constants import posts_host
from kh_common.models.auth import KhUser
from kh_common.hashing import Hashable
from pydantic import parse_obj_as
from typing import List
from models import Post


class Posts(Hashable) :

	Timeout: int = 30

	async def userPosts(self, user: KhUser) -> List[Post] :
		async with async_request(
			'GET',
			f'{posts_host}/v1/fetch_my_posts',
			timeout=ClientTimeout(Posts.Timeout),
			headers={
				'authorization': 'bearer ' + user.token.token_string,
			},
		) as response :
			data = await response.json()

			return parse_obj_as(List[Post], data)
