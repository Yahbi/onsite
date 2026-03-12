#!/usr/bin/env python3
"""Search Socrata Discovery API for datasets in weak states."""
import urllib.request, json

searches = [
    ('building+permits+Denver', 'CO'),
    ('building+permits+Colorado+Springs', 'CO'),
    ('building+permits+Atlanta', 'GA'),
    ('building+permits+Georgia', 'GA'),
    ('building+permits+Portland+Oregon', 'OR'),
    ('building+permits+Oklahoma+City', 'OK'),
    ('building+permits+Tulsa', 'OK'),
    ('building+permits+Minneapolis', 'MN'),
    ('building+permits+Minnesota', 'MN'),
    ('building+permits+Detroit', 'MI'),
    ('building+permits+Grand+Rapids', 'MI'),
    ('building+permits+Washington+DC', 'DC'),
    ('building+permits+Boise', 'ID'),
    ('building+permits+Idaho', 'ID'),
    ('building+permits+Montana', 'MT'),
    ('building+permits+Alaska', 'AK'),
    ('building+permits+New+Hampshire', 'NH'),
    ('building+permits+Seattle', 'WA'),
    ('building+permits+Spokane', 'WA'),
    ('property+sales+Denver', 'CO'),
    ('property+sales+Atlanta', 'GA'),
    ('property+sales+Minneapolis', 'MN'),
    ('property+sales+Detroit', 'MI'),
    ('property+sales+Seattle', 'WA'),
    ('property+transfers+Oregon', 'OR'),
    ('code+enforcement+Denver', 'CO'),
    ('code+enforcement+Atlanta', 'GA'),
    ('code+enforcement+Oklahoma', 'OK'),
    ('code+enforcement+Portland', 'OR'),
    ('construction+permits+South+Carolina', 'SC'),
    ('construction+permits+West+Virginia', 'WV'),
    ('building+permits+Charleston+SC', 'SC'),
    ('building+permits+Greenville+SC', 'SC'),
    ('permits+Anchorage', 'AK'),
    ('permits+Des+Moines', 'IA'),
    ('permits+Iowa', 'IA'),
    ('demolition+permits', ''),
    ('residential+construction+permits', ''),
    ('property+sales+South+Carolina', 'SC'),
    ('property+sales+West+Virginia', 'WV'),
    ('property+sales+Montana', 'MT'),
    ('property+sales+Idaho', 'ID'),
    ('property+sales+Alaska', 'AK'),
    ('property+sales+New+Hampshire', 'NH'),
    ('property+sales+Oklahoma', 'OK'),
    ('property+sales+Georgia', 'GA'),
]

seen = set()
for query, expected_state in searches:
    url = f'https://api.us.socrata.com/api/catalog/v1?q={query}&limit=10'
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            for r in data.get('results', []):
                res = r.get('resource', {})
                rid = res.get('id', '')
                name = res.get('name', '')
                rtype = res.get('type', '')
                domain = r.get('metadata', {}).get('domain', '')
                if rtype != 'dataset' or not rid:
                    continue
                key = f'{domain}/{rid}'
                if key in seen:
                    continue
                seen.add(key)
                name_lower = name.lower()
                good_keywords = ['permit', 'building', 'construction', 'property', 'code enforcement',
                                 'inspection', 'demolition', 'sale', 'transfer', 'parcel', 'assessment']
                if any(kw in name_lower for kw in good_keywords):
                    print(f'{expected_state or "?"}: {domain}/{rid} | {name}')
    except Exception as e:
        pass
