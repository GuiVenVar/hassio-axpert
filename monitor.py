#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, struct, crcmod, errno

DEVICE = os.environ.get("DEVICE", "/dev/hidraw0")

def _build_frame(cmd: str) -> bytes:
    xmodem_crc_func = crcmod.predefined.mkCrcFun('xmodem')
    cb = cmd.encode('ascii')
    crc = xmodem_crc_func(cb)
    crc_b = struct.pack('>H', crc)
    return cb + crc_b + b'\x0d'

def _flush_input(fd: int):
    try:
        while True:
            if not os.read(fd, 512): break
    except OSError:
        pass

def _read_until_cr(fd: int, timeout_s: float = 5.0) -> bytes:
    deadline = time.time() + timeout_s
    r = b''
    while b'\r' not in r:
        if time.time() > deadline:
            raise TimeoutError("Read operation timed out")
        try:
            c = os.read(fd, 128)
            if c: r += c
            else: time.sleep(0.01)
        except OSError as e:
            if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                time.sleep(0.01)
                continue
            raise
    return r

def send_command(cmd: str):
    frame = _build_frame(cmd)
    print(f"\n=== Sending command: {cmd} ===")
    print(f"Frame bytes: {frame.hex()}")

    fd = os.open(DEVICE, os.O_RDWR | os.O_NONBLOCK)
    try:
        _flush_input(fd)
        os.write(fd, frame)
        resp = _read_until_cr(fd, timeout_s=5)
        print(f"Raw response (bytes): {resp}")
        try:
            s = resp.decode('utf-8', errors='ignore')
        except:
            s = resp.decode('iso-8859-1', errors='ignore')
        print(f"Decoded response: {s.strip()}")
    finally:
        os.close(fd)

if __name__ == "__main__":
    commands = ["QID", "QPIGS", "QPIRI", "QPIGS2", "QPGS0"]
    for cmd in commands:
        try:
            send_command(cmd)
        except Exception as e:
            print(f"Error sending {cmd}: {e}")
        time.sleep(1)