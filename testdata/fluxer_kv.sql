-- Схема таблицы, в которую пишет SQL-половина batchGuildAuditLogMessageDeletes.
--
-- Снято с живого fluxer_infra (`\d+ fluxer_kv`, 2026-10-27). Триггер
-- fluxer_kv_sort_id_fill и частичные индексы по messages/message_reactions
-- сюда не переносились: они обслуживают другие table_name, а тест пишет только
-- аудит-записи и читает их обратно.
--
-- Назначение — дать CI одноразовую базу, в которой есть эта таблица. Сам тест
-- (cmd/worker/auditbatch_db_test.go) сеет строки под синтетическим guild_id и
-- убирает их за собой.

CREATE TABLE IF NOT EXISTS fluxer_kv (
    table_name    text        NOT NULL,
    partition_key text        NOT NULL,
    row_key       text        NOT NULL,
    row_data      jsonb       NOT NULL,
    expires_at    timestamptz,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    sort_id       bigint,
    PRIMARY KEY (table_name, row_key)
);

CREATE INDEX IF NOT EXISTS fluxer_kv_expires_idx
    ON fluxer_kv (expires_at)
    WHERE expires_at IS NOT NULL;
