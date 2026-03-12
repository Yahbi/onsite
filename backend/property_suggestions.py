import aiohttp
import os

async def fetch_suggestions(query: str):
    keys = [k for k in [os.getenv("PROPERTYREACH_API_KEY"), os.getenv("PROPERTYREACH_API_KEY_ALT")] if k]
    if not keys:
        return {"success": False, "error": "PropertyReach API key missing"}
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for key in keys:
            try:
                async with session.get(
                    "https://api.propertyreach.com/v1/suggestions",
                    params={"query": query},
                    headers={"x-api-key": key},
                ) as resp:
                    if resp.status != 200:
                        continue
                    return {"success": True, "source": "propertyreach", "data": await resp.json()}
            except Exception:
                continue
    return {"success": False, "error": "suggestions unavailable"}
