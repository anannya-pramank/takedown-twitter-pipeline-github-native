"""
Workaround for a known bug in twitter-cli (github.com/jackwener/twitter-cli).

The ClientTransaction bootstrap (client.py, _ensure_client_transaction)
fetches bare, cookie-less https://x.com to compute the anti-bot
x-client-transaction-id header. As of August 2026, X moved that logged-out
landing page onto a different frontend (a modern ES-module app under
abs.twimg.com/x-web/, no webpack chunk map) while the authenticated /home
page still runs the old responsive-web app the ClientTransaction library
was written against. The bootstrap's regex search for the ",<n>:\"ondemand.s\""
chunk-map entry finds nothing on the new logged-out page, and crashes with
'NoneType' object has no attribute 'group'.

Fix: point the bootstrap at /home with cookies attached instead of bare
x.com with none — that page still runs the app the regex expects, which is
what your own auth_token/ct0 already give you access to for the real API
calls anyway.

Status: not yet fixed upstream as of this writing. Filed at:
  <fill in the issue URL once opened at github.com/jackwener/twitter-cli/issues>

Remove this patch step once a released twitter-cli version no longer needs
it — this script is a no-op (skips cleanly) if the source it's expecting to
find has already changed, so it's safe to leave in place even after an
upstream fix ships; it'll just stop doing anything.
"""
import os
import sys

import twitter_cli

path = os.path.join(os.path.dirname(twitter_cli.__file__), "client.py")
content = open(path, encoding="utf-8").read()

old = '''            home_page = cffi_session.get(
                "https://x.com", headers=ct_headers, timeout=10,
            )'''
new = '''            home_page = cffi_session.get(
                "https://x.com/home", headers=ct_headers,
                cookies={"auth_token": self._auth_token, "ct0": self._ct0},
                timeout=10,
            )'''

if old not in content:
    print("[patch] expected pattern not found in twitter-cli's client.py — "
          "either it's already been fixed upstream, or the source has "
          "changed shape. Skipping patch; no changes made.")
    sys.exit(0)

open(path, "w", encoding="utf-8").write(content.replace(old, new, 1))
print("[patch] twitter-cli ClientTransaction bootstrap patched:", path)
