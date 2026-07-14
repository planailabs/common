//! Self-contained migrations for the generic chat tables.
//!
//! Tracked in this crate's own `_plan_ai_chat_migrations` table so they are
//! fully independent of any host application's migration chain (the host's
//! `sqlx::migrate!()` numbering never collides with these). Append-only:
//! never modify an existing entry, always add a new one.

use anyhow::{Context as _, Result};
use sqlx::PgPool;

/// Ordered, append-only migration list. Names must be unique and stable.
const MIGRATIONS: &[(&str, &str)] = &[(
    "0001_chat_core",
    include_str!("../../migrations/0001_chat_core.sql"),
)];

/// Apply all pending chat migrations. Idempotent; safe to run at every startup.
pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _plan_ai_chat_migrations (\
             name TEXT PRIMARY KEY, \
             applied_at TIMESTAMPTZ NOT NULL DEFAULT now()\
         )",
    )
    .execute(pool)
    .await
    .context("failed to create _plan_ai_chat_migrations table")?;

    for (name, sql) in MIGRATIONS {
        let applied: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM _plan_ai_chat_migrations WHERE name = $1)",
        )
        .bind(name)
        .fetch_one(pool)
        .await?;
        if applied {
            continue;
        }

        let mut tx = pool.begin().await?;
        sqlx::raw_sql(sql)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("chat migration '{name}' failed"))?;
        sqlx::query("INSERT INTO _plan_ai_chat_migrations (name) VALUES ($1)")
            .bind(name)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        tracing::info!(migration = %name, "applied plan-ai-chat migration");
    }
    Ok(())
}
