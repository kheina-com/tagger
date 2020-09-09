from psycopg2.errors import UniqueViolation, ConnectionException
from psycopg2 import Binary, connect as dbConnect
from kh_common import getFullyQualifiedClassName
from kh_common.logging import getLogger
from kh_common.sql import SqlInterface
from typing import List
from io import BytesIO
from math import floor
from PIL import Image


class Uploader(SqlInterface, B2Interface) :

	def __init__(self) :
		SqlInterface.__init__(self)
		B2Interface.__init__(self)
		self.logger = getLogger('tagger')


	def addTags(post_id: str, user_id: int, tags: List[str]) :
		self.query("""
			CALL add_tags(%s, %s, %s);
			""",
			(post_id, user_id, tags,),
			commit=True,
		)
