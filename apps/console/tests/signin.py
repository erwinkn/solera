"""The project of the console tests' second server (playwright.config.ts):
the demo, with a sign-in that makes everyone a reader, so the console must
show them nothing that writes."""

from solera import Identity
from solera_server.demo import build


def reader(request):
    return Identity("reader@example.com", "read")


project = build(sign_in=reader)
