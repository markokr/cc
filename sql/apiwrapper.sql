create schema hots;

create or replace function hots.apiwrapper(json_request text)
returns text as $$
try:
    import json
    pload = json.loads(args[0])
    res = {
        'req': pload['req'],
        'msg': 'Hello from DB'
    }

    return json.dumps(res)
except Exception, d:
    return '{"msg": "db error' + str(d)+'"}'
$$ language plpythonu;
