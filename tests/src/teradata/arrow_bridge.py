#!/usr/bin/env python3
"""
Arrow Flight Bridge Service
Receives data from Teradata C Table Operator via TCP and forwards to Trino via Arrow Flight.

Usage:
    python3 arrow_bridge.py --listen-port 9999 --trino-host 127.0.0.1 --trino-port 50051
"""

import socket
import struct
import threading
import argparse
import json
import sys
import time

import pyarrow as pa
import pyarrow.flight as flight

class ArrowBridge:
    def __init__(self, listen_port, trino_host, trino_port):
        self.listen_port = listen_port
        self.trino_host = trino_host
        self.trino_port = trino_port
        self.running = True
        
    def start(self):
        """Start the bridge server"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('0.0.0.0', self.listen_port))
        server_socket.listen(5)
        print(f"Arrow Bridge listening on port {self.listen_port}", flush=True)
        print(f"Will forward to Trino at {self.trino_host}:{self.trino_port}", flush=True)
        
        while self.running:
            try:
                client_socket, addr = server_socket.accept()
                print(f"Connection from {addr}", flush=True)
                thread = threading.Thread(target=self.handle_client, args=(client_socket, addr))
                thread.daemon = True
                thread.start()
            except Exception as e:
                print(f"Error accepting connection: {e}", flush=True)
                
    def handle_client(self, client_socket, addr):
        """Handle data from C table operator and forward to Trino"""
        writer = None
        client = None
        try:
            # Read query ID
            query_id_len_data = self._recv_exact(client_socket, 4)
            query_id_len = struct.unpack('!I', query_id_len_data)[0]
            query_id = self._recv_exact(client_socket, query_id_len).decode('utf-8')
            print(f"Query ID: {query_id}", flush=True)
            
            # Read schema (as JSON)
            schema_len_data = self._recv_exact(client_socket, 4)
            schema_len = struct.unpack('!I', schema_len_data)[0]
            schema_json = self._recv_exact(client_socket, schema_len).decode('utf-8')
            schema_info = json.loads(schema_json)
            print(f"Schema: {schema_info}", flush=True)
            
            # Build Arrow schema
            fields = []
            for col in schema_info['columns']:
                if col['type'] == 'INTEGER':
                    fields.append(pa.field(col['name'], pa.int32()))
                elif col['type'] == 'BIGINT':
                    fields.append(pa.field(col['name'], pa.int64()))
                elif col['type'] == 'VARCHAR':
                    fields.append(pa.field(col['name'], pa.string()))
                elif col['type'] == 'DOUBLE':
                    fields.append(pa.field(col['name'], pa.float64()))
                else:
                    fields.append(pa.field(col['name'], pa.string()))
            schema = pa.schema(fields)
            
            # Connect to Trino Flight server
            location = flight.Location.for_grpc_tcp(self.trino_host, self.trino_port)
            client = flight.FlightClient(location)
            
            # Start DoPut stream
            descriptor = flight.FlightDescriptor.for_path(query_id)
            writer, _ = client.do_put(descriptor, schema)
            
            total_rows = 0
            batch_count = 0
            
            # Receive and forward batches
            while True:
                # Read batch length
                batch_len_data = self._recv_exact(client_socket, 4)
                batch_len = struct.unpack('!I', batch_len_data)[0]
                
                if batch_len == 0:
                    break
                    
                # Read batch data
                batch_data = self._recv_exact(client_socket, batch_len)
                
                # Parse rows and create Arrow batch
                arrays = self._parse_batch(batch_data, schema_info['columns'])
                batch = pa.record_batch(arrays, schema=schema)
                
                writer.write_batch(batch)
                total_rows += batch.num_rows
                batch_count += 1
                
            writer.close()
            client_socket.sendall(b'OK')
            print(f"Successfully forwarded {total_rows} rows to Trino in {batch_count} batches", flush=True)
            
        except Exception as e:
            print(f"Error handling client {addr}: {e}", flush=True)
        finally:
            if writer:
                try: writer.close()
                except: pass
            if client:
                try: client.close()
                except: pass
            client_socket.close()
            
    def _recv_exact(self, sock, n):
        """Receive exactly n bytes from socket"""
        data = b''
        while len(data) < n:
            chunk = sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed")
            data += chunk
        return data
        
    def _parse_batch(self, data, columns):
        """Parse batch data into Arrow arrays
        
        Format:
        - 4 bytes: num_rows
        - For each row, for each column:
            - 1 byte: null indicator (1=null, 0=not null)
            - If not null:
                - INTEGER: 4 bytes big-endian
                - BIGINT: 8 bytes big-endian
                - VARCHAR: 2 bytes length + data
        """
        offset = 0
        num_rows = struct.unpack('!I', data[offset:offset+4])[0]
        offset += 4
        
        print(f"Parsing batch: {num_rows} rows, {len(columns)} columns", flush=True)
        
        # Initialize lists for each column
        col_data = [[] for _ in columns]
        
        for row_idx in range(num_rows):
            for col_idx, col in enumerate(columns):
                # Read null indicator
                is_null = data[offset]
                offset += 1
                
                if is_null:
                    col_data[col_idx].append(None)
                else:
                    if col['type'] == 'INTEGER':
                        val = struct.unpack('!i', data[offset:offset+4])[0]
                        offset += 4
                        col_data[col_idx].append(val)
                    elif col['type'] == 'BIGINT':
                        val = struct.unpack('!q', data[offset:offset+8])[0]
                        offset += 8
                        col_data[col_idx].append(val)
                    elif col['type'] == 'DOUBLE':
                        val = struct.unpack('!d', data[offset:offset+8])[0]
                        offset += 8
                        col_data[col_idx].append(val)
                    else:  # VARCHAR
                        str_len = struct.unpack('!H', data[offset:offset+2])[0]
                        offset += 2
                        val = data[offset:offset+str_len].decode('utf-8', errors='replace')
                        offset += str_len
                        col_data[col_idx].append(val)
        
        # Convert to Arrow arrays
        arrays = []
        for col_idx, col in enumerate(columns):
            if col['type'] == 'INTEGER':
                arrays.append(pa.array(col_data[col_idx], type=pa.int32()))
            elif col['type'] == 'BIGINT':
                arrays.append(pa.array(col_data[col_idx], type=pa.int64()))
            elif col['type'] == 'DOUBLE':
                arrays.append(pa.array(col_data[col_idx], type=pa.float64()))
            else:
                arrays.append(pa.array(col_data[col_idx], type=pa.string()))
                
        return arrays

def main():
    parser = argparse.ArgumentParser(description='Arrow Flight Bridge Service')
    parser.add_argument('--listen-port', type=int, default=9999, help='Port to listen on')
    parser.add_argument('--trino-host', default='127.0.0.1', help='Trino Flight server host')
    parser.add_argument('--trino-port', type=int, default=50051, help='Trino Flight server port')
    args = parser.parse_args()
    
    bridge = ArrowBridge(args.listen_port, args.trino_host, args.trino_port)
    bridge.start()

if __name__ == '__main__':
    main()
