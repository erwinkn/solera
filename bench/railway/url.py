"""SOLERA_TEST_S3 for the bucket, from its reference variables (bench/railway)."""

import os
from urllib.parse import quote, urlsplit

u = urlsplit(os.environ["ENDPOINT"])
user, secret = quote(os.environ["ACCESS_KEY_ID"], safe=""), quote(os.environ["SECRET_ACCESS_KEY"], safe="")
print(f"{u.scheme}://{user}:{secret}@{u.netloc}/{os.environ['BUCKET']}")
