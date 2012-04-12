create schema hots;

create or replace function hots.apiwrapper(json_request text)
returns text as $$
try:
    import json
except Exception, e:
    return '{"msg": "import error"}'
try:
    payload = json.loads(args[0])
    res = {'req': payload['req']}
except Exception, e:
    return '{"msg": "payload error"}'
try:
    res['msg'] = 'Hello from DB'
except Exception, e:
    res['msg'] = 'db error: %s' % e
return json.dumps(res)
$$ language plpythonu;
