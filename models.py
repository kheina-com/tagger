from typing import List, Optional
from pydantic import BaseModel


class TagsRequest(BaseModel) :
	post_id: str
	tags: List[str]


class InheritRequest(BaseModel) :
	parent_tag: str,
	child_tag: str,
	deprecate: Optional[bool],
	admin: Optional[bool]
