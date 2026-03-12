import { Client } from 'pg';
import { Client as SSHClient } from 'ssh2';
import { readFileSync } from 'fs';
import { postgresConfig, postgresSshConfig, enablePostgresSSH } from '../config/database';
import { Server, Socket } from 'net';
import * as net from 'net';

const TUNNEL_PORT = 5435;
const TUNNEL_HOST = '127.0.0.1';

let client: Client | null = null;
let sshClient: SSHClient | null = null;
let server: Server | null = null;

export async function connectPostgres(): Promise<Client> {
  if (client) {
    return client;
  }

  if (!enablePostgresSSH) {
    return connectDirectly();
  }

  return connectViaSSH();
}

async function connectDirectly(): Promise<Client> {
  console.log('Connecting to PostgreSQL directly (no SSH)');

  client = new Client({
    host: postgresConfig.host,
    port: postgresConfig.port,
    user: postgresConfig.user,
    password: postgresConfig.password,
    database: postgresConfig.database,
    connectionTimeoutMillis: 30000,
  });

  await client.connect();
  console.log('PostgreSQL connection established (direct)');
  return client;
}

async function createSSHConnection(): Promise<SSHClient> {
  return new Promise((resolve, reject) => {
    const ssh = new SSHClient();

    ssh.on('ready', () => {
      console.log('SSH connection established (PostgreSQL)');
      resolve(ssh);
    });

    ssh.on('error', (err) => {
      reject(new Error(`SSH connection failed: ${err.message}`));
    });

    ssh.connect({
      host: postgresSshConfig.host,
      port: postgresSshConfig.port,
      username: postgresSshConfig.username,
      privateKey: readFileSync(postgresSshConfig.privateKeyPath),
      passphrase: postgresSshConfig.passphrase || undefined,
      keepaliveInterval: 10000,
      keepaliveCountMax: 3,
      readyTimeout: 20000,
    });
  });
}

async function keepSSHAlive(ssh: SSHClient): Promise<void> {
  return new Promise((resolve, reject) => {
    ssh.shell((err, _stream) => {
      if (err) {
        reject(new Error(`Failed to start shell session: ${err.message}`));
        return;
      }
      resolve();
    });
  });
}

function handleSocketForwarding(ssh: SSHClient, socket: Socket): void {
  ssh.forwardOut(
    socket.remoteAddress || TUNNEL_HOST,
    socket.remotePort || 0,
    postgresConfig.host,
    postgresConfig.port,
    (err, stream) => {
      if (err) {
        socket.end();
        return;
      }

      // Bidirectional pipe between local socket and SSH stream
      socket.pipe(stream);
      stream.pipe(socket);

      // Handle errors and cleanup
      const cleanup = () => {
        stream.end();
        socket.end();
      };

      socket.on('error', cleanup);
      stream.on('error', cleanup);
      socket.on('close', () => stream.end());
      stream.on('close', () => socket.end());
    }
  );
}

async function createTunnelServer(ssh: SSHClient): Promise<Server> {
  return new Promise((resolve, reject) => {
    const tunnelServer = net.createServer((socket) => {
      handleSocketForwarding(ssh, socket);
    });

    tunnelServer.listen(TUNNEL_PORT, TUNNEL_HOST, () => {
      console.log(`PostgreSQL tunnel ready: ${TUNNEL_HOST}:${TUNNEL_PORT} -> ${postgresConfig.host}:${postgresConfig.port}`);
      resolve(tunnelServer);
    });

    tunnelServer.on('error', (err) => {
      reject(new Error(`Tunnel server failed: ${err.message}`));
    });
  });
}

async function createPostgresConnection(): Promise<Client> {
  // Wait a bit for tunnel to be fully ready
  await new Promise(resolve => setTimeout(resolve, 500));

  const pgClient = new Client({
    host: TUNNEL_HOST,
    port: TUNNEL_PORT,
    user: postgresConfig.user,
    password: postgresConfig.password,
    database: postgresConfig.database,
    connectionTimeoutMillis: 30000,
  });

  await pgClient.connect();
  console.log('PostgreSQL connection established (via SSH)');
  return pgClient;
}

async function connectViaSSH(): Promise<Client> {
  try {
    // Step 1: Establish SSH connection
    sshClient = await createSSHConnection();

    // Step 2: Keep SSH connection alive
    await keepSSHAlive(sshClient);

    // Step 3: Create local tunnel server
    server = await createTunnelServer(sshClient);

    // Step 4: Connect to PostgreSQL through tunnel
    client = await createPostgresConnection();

    return client;
  } catch (error) {
    // Cleanup on error
    await disconnectPostgres();
    throw error;
  }
}

export async function disconnectPostgres(): Promise<void> {
  if (client) {
    await client.end();
    client = null;
    console.log('PostgreSQL connection closed');
  }

  if (server) {
    server.close();
    server = null;
    console.log('Local tunnel server closed (PostgreSQL)');
  }

  if (sshClient) {
    sshClient.end();
    sshClient = null;
    console.log('SSH connection closed (PostgreSQL)');
  }
}

export function getPostgresClient(): Client {
  if (!client) {
    throw new Error('PostgreSQL connection not established. Call connectPostgres() first.');
  }
  return client;
}

