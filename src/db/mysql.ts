import mysql from 'mysql2/promise';
import { Client } from 'ssh2';
import { readFileSync } from 'fs';
import { mysqlConfig, mysqlSshConfig } from '../config/database';
import { Server, Socket } from 'net';
import * as net from 'net';

const TUNNEL_PORT = 3307;
const TUNNEL_HOST = '127.0.0.1';

let connection: mysql.Connection | null = null;
let sshClient: Client | null = null;
let server: Server | null = null;

async function createSSHConnection(): Promise<Client> {
  return new Promise((resolve, reject) => {
    const client = new Client();

    client.on('ready', () => {
      console.log('SSH connection established');
      resolve(client);
    });

    client.on('error', (err) => {
      reject(new Error(`SSH connection failed: ${err.message}`));
    });

    client.connect({
      host: mysqlSshConfig.host,
      port: mysqlSshConfig.port,
      username: mysqlSshConfig.username,
      privateKey: readFileSync(mysqlSshConfig.privateKeyPath),
      passphrase: mysqlSshConfig.passphrase || undefined,
      keepaliveInterval: 10000,
      keepaliveCountMax: 3,
      readyTimeout: 20000,
    });
  });
}

async function keepSSHAlive(client: Client): Promise<void> {
  return new Promise((resolve, reject) => {
    client.shell((err, _stream) => {
      if (err) {
        reject(new Error(`Failed to start shell session: ${err.message}`));
        return;
      }
      resolve();
    });
  });
}

function handleSocketForwarding(client: Client, socket: Socket): void {
  client.forwardOut(
    socket.remoteAddress || TUNNEL_HOST,
    socket.remotePort || 0,
    mysqlConfig.host,
    mysqlConfig.port,
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

async function createTunnelServer(client: Client): Promise<Server> {
  return new Promise((resolve, reject) => {
    const tunnelServer = net.createServer((socket) => {
      handleSocketForwarding(client, socket);
    });

    tunnelServer.listen(TUNNEL_PORT, TUNNEL_HOST, () => {
      console.log(`MySQL tunnel ready: ${TUNNEL_HOST}:${TUNNEL_PORT} -> ${mysqlConfig.host}:${mysqlConfig.port}`);
      resolve(tunnelServer);
    });

    tunnelServer.on('error', (err) => {
      reject(new Error(`Tunnel server failed: ${err.message}`));
    });
  });
}

async function createMySQLConnection(): Promise<mysql.Connection> {
  // Wait a bit for tunnel to be fully ready
  await new Promise(resolve => setTimeout(resolve, 500));

  const conn = await mysql.createConnection({
    host: TUNNEL_HOST,
    port: TUNNEL_PORT,
    user: mysqlConfig.user,
    password: mysqlConfig.password,
    database: mysqlConfig.database,
    connectTimeout: 30000,
    ssl: {
      rejectUnauthorized: false,
    },
  });

  console.log('MySQL connection established');
  return conn;
}

export async function connectMySQL(): Promise<mysql.Connection> {
  if (connection) {
    return connection;
  }

  try {
    // Step 1: Establish SSH connection
    sshClient = await createSSHConnection();

    // Step 2: Keep SSH connection alive
    await keepSSHAlive(sshClient);

    // Step 3: Create local tunnel server
    server = await createTunnelServer(sshClient);

    // Step 4: Connect to MySQL through tunnel
    connection = await createMySQLConnection();

    return connection;
  } catch (error) {
    // Cleanup on error
    await disconnectMySQL();
    throw error;
  }
}

export async function disconnectMySQL(): Promise<void> {
  if (connection) {
    await connection.end();
    connection = null;
    console.log('MySQL connection closed');
  }

  if (server) {
    server.close();
    server = null;
    console.log('Local tunnel server closed');
  }

  if (sshClient) {
    sshClient.end();
    sshClient = null;
    console.log('SSH connection closed');
  }
}

export function getMySQLConnection(): mysql.Connection {
  if (!connection) {
    throw new Error('MySQL connection not established. Call connectMySQL() first.');
  }
  return connection;
}

