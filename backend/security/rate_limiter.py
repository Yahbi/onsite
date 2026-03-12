"""
Rate Limiting Middleware for Onsite
Pure ASGI middleware — avoids BaseHTTPMiddleware overhead.
"""

from fastapi import Request
from fastapi.responses import JSONResponse
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Tuple
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Pure ASGI rate limiting middleware.

    Features:
    - Per-IP rate limiting
    - Per-user rate limiting (if authenticated)
    - Configurable limits for different endpoints
    - Automatic cleanup of old entries
    """

    def __init__(
        self,
        app,
        requests_per_minute: int = 60,
        requests_per_hour: int = 1000,
        enabled: bool = True
    ):
        self.app = app
        self.enabled = enabled
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour

        # Storage: {identifier: [timestamp, ...]}
        self.minute_buckets: Dict[str, list] = defaultdict(list)
        self.hour_buckets: Dict[str, list] = defaultdict(list)

        # Endpoint-specific limits
        self.endpoint_limits = {
            "/api/sync": (10, 100),
            "/api/enrich": (20, 200),
            "/api/leads": (60, 1000),
        }

        logger.info(f"Rate limiter initialized: {requests_per_minute}/min, {requests_per_hour}/hour")

    def get_identifier(self, scope) -> str:
        request = Request(scope)
        if hasattr(request.state, "user_id") and request.state.user_id:
            return f"user:{request.state.user_id}"
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            client_ip = forwarded_for.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client else "unknown"
        return f"ip:{client_ip}"

    def get_limits(self, path: str) -> Tuple[int, int]:
        for endpoint_path, limits in self.endpoint_limits.items():
            if path.startswith(endpoint_path):
                return limits
        return (self.requests_per_minute, self.requests_per_hour)

    def cleanup_old_entries(self, bucket: Dict[str, list], max_age: timedelta):
        now = datetime.now()
        for identifier in list(bucket.keys()):
            bucket[identifier] = [
                ts for ts in bucket[identifier]
                if now - ts < max_age
            ]
            if not bucket[identifier]:
                del bucket[identifier]

    def is_rate_limited(self, identifier: str, path: str) -> Tuple[bool, str]:
        now = datetime.now()
        per_minute, per_hour = self.get_limits(path)

        self.cleanup_old_entries(self.minute_buckets, timedelta(minutes=1))
        self.cleanup_old_entries(self.hour_buckets, timedelta(hours=1))

        minute_requests = [
            ts for ts in self.minute_buckets[identifier]
            if now - ts < timedelta(minutes=1)
        ]
        if len(minute_requests) >= per_minute:
            return True, f"Rate limit exceeded: {per_minute} requests per minute"

        hour_requests = [
            ts for ts in self.hour_buckets[identifier]
            if now - ts < timedelta(hours=1)
        ]
        if len(hour_requests) >= per_hour:
            return True, f"Rate limit exceeded: {per_hour} requests per hour"

        self.minute_buckets[identifier].append(now)
        self.hour_buckets[identifier].append(now)

        return False, ""

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http",):
            await self.app(scope, receive, send)
            return

        if not self.enabled:
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path in ["/health", "/", "/favicon.ico"]:
            await self.app(scope, receive, send)
            return

        identifier = self.get_identifier(scope)
        is_limited, reason = self.is_rate_limited(identifier, path)

        if is_limited:
            logger.warning(f"Rate limit exceeded for {identifier} on {path}")
            response = JSONResponse(
                status_code=429,
                content={
                    "error": "Too Many Requests",
                    "message": reason,
                    "retry_after": "60 seconds"
                },
                headers={
                    "Retry-After": "60",
                    "X-RateLimit-Limit": str(self.requests_per_minute),
                    "X-RateLimit-Remaining": "0"
                }
            )
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)


def get_rate_limiter_stats() -> Dict:
    return {
        "total_identifiers_tracked": 0,
        "requests_last_minute": 0,
        "requests_last_hour": 0,
        "top_requesters": []
    }
