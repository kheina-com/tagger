from datetime import datetime
from enum import Enum, unique
from typing import Dict, List, Optional

from kh_common.models.privacy import Privacy
from kh_common.models.rating import Rating
from kh_common.models.user import UserPortable
from pydantic import BaseModel


class LookupRequest(BaseModel) :
	tag: Optional[str]


class PostRequest(BaseModel) :
	post_id: str


class TagsRequest(PostRequest) :
	tags: List[str]


class RemoveInheritance(BaseModel) :
	parent_tag: str
	child_tag: str


class InheritRequest(RemoveInheritance) :
	deprecate: Optional[bool] = False


class UpdateRequest(BaseModel) :
	tag: str
	name: Optional[str]
	tag_class: Optional[str]
	owner: Optional[str]
	description: Optional[str]
	deprecated: Optional[bool] = None


@unique
class TagGroupPortable(Enum) :
	artist: str = 'artist'
	subject: str = 'subject'
	sponsor: str = 'sponsor'
	species: str = 'species'
	gender: str = 'gender'
	misc: str = 'misc'


class TagPortable(str) :
	pass


class TagGroups(Dict[TagGroupPortable, List[TagPortable]]) :
	pass


class Tag(BaseModel) :
	tag: str
	owner: Optional[UserPortable]
	group: TagGroupPortable
	deprecated: bool
	inherited_tags: List[TagPortable]
	description: Optional[str]
	count: int


class Score(BaseModel) :
	up: int
	down: int
	total: int
	user_vote: Optional[int]


class MediaType(BaseModel) :
	file_type: str
	mime_type: str

class Post(BaseModel) :
	post_id: str
	title: Optional[str]
	description: Optional[str]
	user: UserPortable
	score: Optional[Score]
	rating: Rating
	parent: Optional[str]
	privacy: Privacy
	created: Optional[datetime]
	updated: Optional[datetime]
	filename: Optional[str]
	media_type: Optional[MediaType]
	blocked: bool
