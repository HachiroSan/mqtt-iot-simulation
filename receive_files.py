#!/usr/bin/env python3
"""
Run a subscriber that receives chunked files over MQTT and writes them to disk.
"""

import argparse
import logging
import signal
import sys
import time

from file_transfer import ChunkedFileSubscriber


def main():
    parser = argparse.ArgumentParser(description="Start MQTT file receiver (subscriber)")
    parser.add_argument("--storage-dir", default=".transfer", help="Directory to store incoming files and state")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    receiver = ChunkedFileSubscriber(storage_dir=args.storage_dir)

    def handle_sigint(signum, frame):
        print("\nStopping subscriber...")
        try:
            receiver.subscriber.disconnect()
        finally:
            sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)

    print("Connecting subscriber to broker and subscribing to file topics...")
    receiver.start()
    print(f"Receiver running. Storage directory: {args.storage_dir}")
    print("Press Ctrl+C to stop.")

    # Keep the process alive while MQTT network loop runs in background
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()


