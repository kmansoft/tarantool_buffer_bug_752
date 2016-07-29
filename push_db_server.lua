#!/usr/bin/env tarantool

os = require("os")
fio = require("fio")
json = require("json")

--[[
We use millisecond timestamps (passed in by caller)
--]]
local TIME_MS_SECOND = 1000ULL
local TIME_MS_10_MIN = TIME_MS_SECOND * 60ULL * 10ULL
local TIME_MS_30_MIN = TIME_MS_SECOND * 60ULL * 30ULL
local TIME_MS_1_HOUR = TIME_MS_SECOND * 60ULL * 60ULL
local TIME_MS_2_HOUR = 2ULL * TIME_MS_1_HOUR
local TIME_MS_4_HOUR = 4ULL * TIME_MS_1_HOUR

--[[
Startup info
]]--
print('Push database starting up')
print('Lua version: ', _VERSION)

--[[
Config file
]]--

config_file_name = os.getenv("PUSH_CONFIG_FILE")
if not config_file_name
then
	config_file_name = "push_config_debug.json"
end

config_file = fio.open(config_file_name)
if not config_file
then
	print('Could not open '..config_file_name..', exiting')
	os.exit()
end

config_data = config_file:read(16 * 1024)
if not config_data
then
	print('Could not read '..config_file_name..', exiting')
	os.exit()
end

config_json = json.decode(config_data)
if not config_json or not config_json.db
then
	print('Could not parse '..config_file_name..', exiting')
	os.exit()
end

local config_db = config_json.db
local CONFIG_BIND = config_db.bind

if not CONFIG_BIND
then
	CONFIG_BIND = "127.0.0.1:60501"
end

config_file:close()

--[[
Database config
]]
box.cfg {
	logger = 'push_db_server.log',
	log_level = 5,
	snapshot_period = 3600,
	snapshot_count = 3,
	listen = CONFIG_BIND
}

--[[

1.6.8-752-g8fc147c issue

https://groups.google.com/forum/#!topic/tarantool/j7f3l7xPqvA

readahead = 323200

--]]

print('Listening on:', CONFIG_BIND)

local user = box.session.user()
print('Current user:', user)

--[[
"subs" space
--]]

space_subs = box.space.subs
if not space_subs then
    space_subs = box.schema.space.create('subs')
    space_subs:create_index('primary', { parts = {1, 'STR'}, type = 'HASH' })
    space_subs:create_index('dev_id', { parts = {2, 'STR'}, type = 'TREE', unique = false})
    space_subs:create_index('ping_ts', { parts = {3, 'NUM'}, type = 'TREE', unique = false })
end

--[[
"devs" space
--]]

space_devs = box.space.devs
if not space_devs then
    space_devs = box.schema.space.create('devs')
    space_devs:create_index('primary', { parts = {1, 'STR'}, type = 'HASH' })
    space_devs:create_index('ping_ts', { parts = {5, 'NUM'}, type = 'TREE', unique = false})
    space_devs:create_index('change_ts', { parts = {6, 'NUM'}, type = 'TREE', unique = false })
end

--[[
Access
--]]

if config_db.user and config_db.pass
then
	local funcs = {
		'push_CreateDev',
		'push_CreateSub',
		'push_DeleteSub',
		'push_PingSub',
		'push_ChangeSub',
		'push_GetSubsByDevice'
	}

	local user = config_db.user
	local pass = config_db.pass
	local user_key = 'access-user-'..user

	print('Access control:', 'user '..user..', key '..user_key)

	box.once(user_key..'-user', function()
			box.schema.user.create(user)
			box.schema.user.grant(user, 'read,write', 'space', 'subs')
			box.schema.user.grant(user, 'read,write', 'space', 'devs')
		end)
	for i, f in ipairs(funcs)
	do
		box.once(user_key..'-func-'..f, function()
				box.schema.func.create(f, { if_not_exists = true })
				box.schema.user.grant(user, 'execute', 'function', f)
			end)
	end

	box.schema.user.passwd(user, pass)
else
	print('Access control:', 'guest')
	box.once('access-guest', function()
			box.schema.user.grant('guest', 'read,write,execute', 'universe', nil)
		end)
end

--[[
Stats
--]]
print ('Existing devs count: ', space_devs:len())
print ('Existing subs count: ', space_subs:len())

--[[
Error codes
--]]

local RES_OK = 0

local RES_ERR_UNKNOWN_DEV_ID = -1
local RES_ERR_UNKNOWN_SUB_ID = -2
local RES_ERR_MISMATCHING_SUB_ID_DEV_ID = -3

--[[
Device registration (cleanup is automatic, time based)
--]]

function push_CreateDev(dev_id, auth, push_token, push_tech, now)
	local t_dev = {dev_id, auth, push_token, push_tech, now, now - TIME_MS_1_HOUR, 0}
	-- dev.ping_ts: now
	space_devs:upsert(t_dev, {{'=', 5, now}})

	return space_devs:get(dev_id)
end

--[[
Create a sub
--]]

function push_CreateSub(dev_id, sub_id, now)
	local res = RES_OK

	-- dev.ping_ts: now
	local t_dev = space_devs:update(dev_id, {{'=', 5, now}})

	if t_dev == nil
	then
		res = RES_ERR_UNKNOWN_DEV_ID
	else
		-- sub.ping_ts: now + 10 min, change_ts: now - 1 hour
		t_sub = {sub_id, dev_id, now + TIME_MS_10_MIN, now - TIME_MS_1_HOUR}
		space_subs:upsert(t_sub, {{'=', 3, now + TIME_MS_10_MIN}})
	end

	return res
end

--[[
Sub ping / change
--]]

function push_PingSub(dev_id, sub_id, set_ping_ts)
	local res = RES_OK

	-- Update sub.ping_ts
	local t_sub = space_subs:update(sub_id, {{'=', 3, set_ping_ts}})

	if t_sub == nil
	then
		res = RES_ERR_UNKNOWN_SUB_ID
	elseif t_sub[2] ~= dev_id
	then
		res = RES_ERR_MISMATCHING_SUB_ID_DEV_ID
	end

	return res
end

function push_ChangeSub(dev_id, sub_id, set_change_ts)
	local res = RES_OK

	-- Update sub.ping_ts, sub.change_ts
	local t_sub = space_subs:update(sub_id, {{'=', 3, set_change_ts}, {'=', 4, set_change_ts}})

	if t_sub == nil
	then
		res = RES_ERR_UNKNOWN_SUB_ID
	elseif t_sub[2] ~= dev_id
	then
		res = RES_ERR_MISMATCHING_SUB_ID_DEV_ID
	else
		-- box.begin()

		-- Update dev.change_ts
		space_devs:update(dev_id, {{'=', 6, set_change_ts}})

		-- box.commit()
	end

	return res
end
