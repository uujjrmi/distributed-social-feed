#!/usr/bin/env python3
import json
import os
import random
import time
import urllib.error
import urllib.request
from pathlib import Path


USER_SERVICE = os.getenv("USER_SERVICE_URL", "http://localhost:8001")
POST_SERVICE = os.getenv("POST_SERVICE_URL", "http://localhost:8002")
USERS = int(os.getenv("SEED_USERS", "100"))
FOLLOWS_PER_USER = int(os.getenv("SEED_FOLLOWS_PER_USER", "10"))
POSTS = int(os.getenv("SEED_POSTS", "300"))


def request(method, url, body=None, timeout=10):
    data = None
    headers = {"content-type": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        raw = response.read()
        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))


def wait_for(url, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            request("GET", url, timeout=3)
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"timed out waiting for {url}")


def create_user(index):
    username = f"user_{index:05d}"
    try:
        return request(
            "POST",
            f"{USER_SERVICE}/users",
            {"username": username, "display_name": f"User {index:05d}"},
        )
    except urllib.error.HTTPError as exc:
        if exc.code != 409:
            raise
        raise RuntimeError(
            "seed users already exist; reset the database with `docker compose down -v` "
            "or change SEED_USERS/username prefix"
        ) from exc


def main():
    wait_for(f"{USER_SERVICE}/healthz")
    wait_for(f"{POST_SERVICE}/healthz")

    print(f"creating {USERS} users")
    users = [create_user(i) for i in range(USERS)]
    user_ids = [user["id"] for user in users]

    print(f"creating {FOLLOWS_PER_USER} follows per user")
    for i, follower_id in enumerate(user_ids):
        for offset in range(1, FOLLOWS_PER_USER + 1):
            followee_id = user_ids[(i + offset) % len(user_ids)]
            request("POST", f"{USER_SERVICE}/users/{follower_id}/follow/{followee_id}")

    print(f"creating {POSTS} posts")
    for i in range(POSTS):
        author_id = random.choice(user_ids)
        request(
            "POST",
            f"{POST_SERVICE}/posts",
            {
                "author_id": author_id,
                "content": f"seed post {i:05d} from {author_id}",
            },
        )

    output = Path(__file__).resolve().parents[1] / "load" / "seed-users.json"
    output.write_text(json.dumps({"users": user_ids}, indent=2), encoding="utf-8")
    print(f"wrote {output}")
    print("seed complete")


if __name__ == "__main__":
    main()

