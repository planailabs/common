//! Self-contained migrations for the `action_template_runs` table.
//!
//! Tracked in this crate's own `_plan_ai_actions_migrations` table so they
//! are fully independent of any host application's migration chain (the
//! host's `sqlx::migrate!()` numbering never collides with these).
//! Append-only: never modify an existing entry, always add a new one.

use anyhow::{Context as _, Result};
use sqlx::PgPool;

/// Ordered, append-only migration list. Names must be unique and stable.
const MIGRATIONS: &[(&str, &str)] = &[(
    "0001_action_template_runs",
    include_str!("../migrations/0001_action_template_runs.sql"),
)];

/// Apply all pending action migrations. Idempotent; safe to run at every
/// startup. Run AFTER the host's own migrations (hosts that previously owned
/// the table drop it there before this recreates it).
pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _plan_ai_actions_migrations (\
             name TEXT PRIMARY KEY, \
             applied_at TIMESTAMPTZ NOT NULL DEFAULT now()\
         )",
    )
    .execute(pool)
    .await
    .context("failed to create _plan_ai_actions_migrations table")?;

    for (name, sql) in MIGRATIONS {
        let applied: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM _plan_ai_actions_migrations WHERE name = $1)",
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
            .with_context(|| format!("actions migration '{name}' failed"))?;
        sqlx::query("INSERT INTO _plan_ai_actions_migrations (name) VALUES ($1)")
            .bind(name)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        tracing::info!(migration = %name, "applied plan-ai-actions migration");
    }
    Ok(())
}
