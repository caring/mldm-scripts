# MLDM Scripts

Data migration scripts to move data from legacy DIR (MySQL) to Modular Monolith (PostgreSQL).

**Epic**: https://caring.atlassian.net/browse/CARE-1721
**Ticket**: https://caring.atlassian.net/browse/CARE-1726

## What We're Migrating

1. **Call History** - Legacy Talk contact history
2. **Notes** - Affiliate notes (self-qualified notes intentionally archived)

## Setup

1. Install dependencies:
```bash
npm install
```

2. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your credentials
```

## Database Connections

### MySQL (Legacy DIR - Production Only)
- Always connects via SSH tunnel to production database
- Source: `dir_production` database
- Cannot be disabled (production data only)

### PostgreSQL (Modular Monolith - Production or Local)
- **Production Mode** (`ENABLE_POSTGRES_SSH=true`): Connects via SSH tunnel to production
- **Local Mode** (`ENABLE_POSTGRES_SSH=false`): Connects directly to local PostgreSQL for testing

## Configuration

### Production Mode (Default)
```env
ENABLE_POSTGRES_SSH=true
POSTGRES_HOST=canario-db-replica.csokkjcgv0yx.us-east-1.rds.amazonaws.com
POSTGRES_PORT=5432
```

### Local Testing Mode
```env
ENABLE_POSTGRES_SSH=false
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DATABASE=modular_monolith
```

See `.env.local.example` for a complete local testing configuration.

## Usage

### Development
```bash
npm run dev
```

### Build
```bash
npm run build
```

### Run Compiled
```bash
npm start
```

## Architecture

- `src/config/database.ts` - Database configuration
- `src/db/mysql.ts` - MySQL connection with SSH tunnel
- `src/db/postgres.ts` - PostgreSQL connection (with optional SSH tunnel)
- `src/index.ts` - Main entry point


---

ssh -o StrictHostKeyChecking=no -L 5435:canario-db-replica.csokkjcgv0yx.us-east-1.rds.amazonaws.com:5432 ec2-user@canary-bastion.caring.com -i ~/.ssh/hsarapalle -N


# stage
POSTGRES_HOST=canario-db-replica.csokkjcgv0yx.us-east-1.rds.amazonaws.com
POSTGRES_PORT=5432
POSTGRES_USER=hsarapalle
POSTGRES_PASSWORD=dfjhdsbf93f
POSTGRES_DATABASE=monolith

# prod 
POSTGRES_HOST=canario-db-replica.csokkjcgv0yx.us-east-1.rds.amazonaws.com
POSTGRES_PORT=5432
POSTGRES_USER=hsarapalle
POSTGRES_PASSWORD=dfjhdsbf93f
POSTGRES_DATABASE=monolith