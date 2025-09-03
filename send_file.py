#!/usr/bin/env python3
"""
CLI tool to send a file over MQTT using chunked transfer.
"""

import argparse
import logging
from file_transfer import ChunkedFilePublisher


def main():
    parser = argparse.ArgumentParser(description="Send a file over MQTT in chunks")
    parser.add_argument("path", help="Path to the file to send")
    parser.add_argument("--chunk-size", type=int, default=256 * 1024, help="Chunk size in bytes (default 256KB)")
    parser.add_argument("--qos", type=int, default=None, help="MQTT QoS level (0,1,2)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    publisher = ChunkedFilePublisher()
    file_id = publisher.send_file(args.path, chunk_size=args.chunk_size, qos=args.qos)
    print(f"File enqueued for transfer with id: {file_id}")


if __name__ == "__main__":
    main()


