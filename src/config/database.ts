import dotenv from 'dotenv';

dotenv.config();

// Feature flags
export const enablePostgresSSH = process.env.ENABLE_POSTGRES_SSH === 'true';

// MySQL SSH Tunnel Configuration
export const mysqlSshConfig = {
  host: process.env.MYSQL_SSH_HOST || '',
  port: parseInt(process.env.MYSQL_SSH_PORT || '22'),
  username: process.env.MYSQL_SSH_USER || '',
  privateKeyPath: process.env.MYSQL_SSH_PRIVATE_KEY_PATH || '',
  passphrase: process.env.MYSQL_SSH_PASSPHRASE || undefined,
};

// MySQL Database Configuration
export const mysqlConfig = {
  host: process.env.MYSQL_HOST || '',
  port: parseInt(process.env.MYSQL_PORT || '3306'),
  user: process.env.MYSQL_USER || '',
  password: process.env.MYSQL_PASSWORD || '',
  database: process.env.MYSQL_DATABASE || '',
};

// PostgreSQL SSH Tunnel Configuration
export const postgresSshConfig = {
  host: process.env.POSTGRES_SSH_HOST || '',
  port: parseInt(process.env.POSTGRES_SSH_PORT || '22'),
  username: process.env.POSTGRES_SSH_USER || '',
  privateKeyPath: process.env.POSTGRES_SSH_PRIVATE_KEY_PATH || '',
  passphrase: process.env.POSTGRES_SSH_PASSPHRASE || undefined,
};

// PostgreSQL Database Configuration
export const postgresConfig = {
  host: process.env.POSTGRES_HOST || '',
  port: parseInt(process.env.POSTGRES_PORT || '5432'),
  user: process.env.POSTGRES_USER || '',
  password: process.env.POSTGRES_PASSWORD || '',
  database: process.env.POSTGRES_DATABASE || '',
};

