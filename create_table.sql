CREATE TABLE IF NOT EXISTS odata_tasks_generator (
    id serial4 NOT NULL,
    "sql" text NULL,
    avro_schema jsonb NULL,
    directory_path text NULL,
    topic_name text NULL,
    batch_size int4 NULL,
    task_name text NULL,
    is_enabled bool NULL,
    task_format varchar DEFAULT 'avro'::character varying NOT NULL,
    CONSTRAINT odata_tasks_generator_pk PRIMARY KEY (id)
    );

truncate odata_tasks_generator;

INSERT INTO public.odata_tasks_generator ("sql",avro_schema,directory_path,topic_name,batch_size,task_name,is_enabled,task_format) VALUES
	('select 1372117011811012618 as server_id','{"name": "DsChannels", "type": "record", "fields": [{"name": "server_id", "type": "int"}, {"name": "channels", "type": ["null", "string"]}]}','ds_channels_ids','ds_channels_tasks',10,'ds_channels',true,'avro'),
	('select 1 as channel_id;','{"name": "TGchannels", "type": "record", "fields": [{"name": "channel_id", "type": "long"}]}','tg_channels_ids','tg_channels_tasks',10,'tg_channels',true,'avro'),
	('select 1372117011811012618 as server_id','{"name": "DsChannels", "type": "record", "fields": [{"name": "server_id", "type": "int"}, {"name": "channels", "type": ["null", "string"]}]}','vk_groups_ids','vk_groups_tasks',10,'vk_groups',true,'avro'),
	('select 1 as channel_id;','{"name": "TGchannels", "type": "record", "fields": [{"name": "channel_id", "type": "long"}]}','ok_friendings_ids','ok_friendings_tasks',10,'ok_friendings',true,'avro'),
    ('select 1372117011811012618 as server_id','{"name": "DsChannels", "type": "record", "fields": [{"name": "server_id", "type": "int"}, {"name": "channels", "type": ["null", "string"]}]}','ds_messages_ids','ds_messages_tasks',10,'ds_messages',true,'avro'),
	('select 1 as channel_id;','{"name": "TGchannels", "type": "record", "fields": [{"name": "channel_id", "type": "long"}]}','vk_users_ids','vk_users_tasks',10,'vk_users',true,'avro');

-- cat create_table.sql | docker exec -i postgres psql -U postgres -d postgres