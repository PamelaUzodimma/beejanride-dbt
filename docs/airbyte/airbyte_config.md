# Airbyte Configuration

## Source: BeejanRide PostgreSQL (Supabase)
| Field | Value |
|-------|-------|
| Connector | PostgreSQL |
| Host | aws-1-eu-west-1.pooler.supabase.com |
| Port | 6543 |
| Database | postgres |
| Username | postgres.jzwdfotpgikjhmmyojok |
| SSL Mode | require |
| Schema | public |

## Destination: Google BigQuery
| Field | Value |
|-------|-------|
| Connector | BigQuery |
| Project ID | beejan-analytics |
| Dataset ID | beejanride_raw |
| Location | US |
| Auth Method | Service Account JSON |

## Connection Settings
| Table | Sync Mode | Frequency |
|-------|-----------|-----------|
| trips_raw | Full Refresh / Overwrite | Manual |
| drivers_raw | Full Refresh / Overwrite | Manual |
| riders_raw | Full Refresh / Overwrite | Manual |
| payments_raw | Full Refresh / Overwrite | Manual |
| cities_raw | Full Refresh / Overwrite | Manual |
| driver_status_events_raw | Incremental / Append | Manual |

## Note on Seed Data
Due to network restrictions during development, dbt seed files were used
to simulate Airbyte ingestion into the beejanride_raw schema. The Airbyte
configuration above represents the production ingestion design.
